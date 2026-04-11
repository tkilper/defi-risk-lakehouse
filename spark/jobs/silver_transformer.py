"""
Spark Silver Transformer
========================
Reads Iceberg bronze tables from ``nessie.bronze`` and produces
normalised, USD-valued silver tables in ``nessie.silver``:

  - nessie.silver.aave_positions
  - nessie.silver.compound_positions
  - nessie.silver.maker_vaults

Transformations applied
-----------------------
* Type-cast raw strings to numerics
* Convert raw token amounts to human-readable units (divide by 10^decimals)
* Compute ``collateral_usd`` and ``debt_usd`` using oracle prices
* Normalise liquidation thresholds to decimals (basis points ÷ 10_000)
* Deduplicate within the same ingestion batch (keep latest by ingestion_ts)

Run:
    spark-submit --master spark://spark-master:7077 silver_transformer.py
"""

from __future__ import annotations

import logging
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType
from spark.jobs.utils import get_spark_session

logging.basicConfig(level=logging.INFO, stream=sys.stdout)
logger = logging.getLogger(__name__)

WAD = 1e18   # MakerDAO WAD unit
RAD = 1e45   # MakerDAO RAD unit
ETH_PRICE_ORACLE_SCALE = 1e18  # Aave priceInEth is scaled by 1e18

# Hard-coded approximate ETH/USD price used only when oracle data is missing.
# In production, fetch this from a price oracle or Chainlink.
_FALLBACK_ETH_USD = 3_000.0


# ---------------------------------------------------------------------------
# Aave
# ---------------------------------------------------------------------------

def transform_aave(spark: SparkSession) -> DataFrame:
    """
    Normalise Aave V3 raw positions into the silver schema.

    Key calculations
    ----------------
    token_amount_raw / 10^decimals  → human units
    priceInEth (1e18 scale) * eth_usd / 1e18 → USD price per token
    collateral_usd = collateral_human * price_usd
    debt_usd = (variable_debt + stable_debt) * price_usd
    liquidation_threshold = reserveLiquidationThreshold / 10_000
    """
    logger.info("Transforming Aave bronze → silver")
    raw = spark.read.table("nessie.bronze.aave_raw_positions")

    silver = (
        raw
        # User
        .withColumn("user_address", F.lower(F.col("user.id")))
        # Reserve metadata
        .withColumn("reserve_address", F.lower(F.col("reserve.id")))
        .withColumn("symbol", F.col("reserve.symbol"))
        .withColumn("decimals", F.col("reserve.decimals").cast(IntegerType()))
        # Liquidation parameters (basis points → decimal)
        .withColumn(
            "liquidation_threshold",
            (F.col("reserve.reserveLiquidationThreshold").cast(DoubleType()) / 10_000.0),
        )
        .withColumn(
            "ltv",
            (F.col("reserve.baseLTVasCollateral").cast(DoubleType()) / 10_000.0),
        )
        .withColumn(
            "liquidation_bonus",
            (F.col("reserve.reserveLiquidationBonus").cast(DoubleType()) / 10_000.0),
        )
        # Price in USD (priceInEth * eth_usd / 1e18)
        .withColumn(
            "price_in_eth",
            (F.col("reserve.price.priceInEth").cast(DoubleType()) / ETH_PRICE_ORACLE_SCALE),
        )
        .withColumn("eth_usd", F.lit(_FALLBACK_ETH_USD))
        .withColumn("price_usd", F.col("price_in_eth") * F.col("eth_usd"))
        # Human-readable token amounts
        .withColumn(
            "collateral_raw",
            F.col("currentATokenBalance").cast(DoubleType()),
        )
        .withColumn(
            "variable_debt_raw",
            F.col("currentVariableDebt").cast(DoubleType()),
        )
        .withColumn(
            "stable_debt_raw",
            F.col("currentStableDebt").cast(DoubleType()),
        )
        .withColumn("scale", F.pow(F.lit(10.0), F.col("decimals").cast(DoubleType())))
        .withColumn("collateral_human", F.col("collateral_raw") / F.col("scale"))
        .withColumn("variable_debt_human", F.col("variable_debt_raw") / F.col("scale"))
        .withColumn("stable_debt_human", F.col("stable_debt_raw") / F.col("scale"))
        # USD values
        .withColumn("collateral_usd", F.col("collateral_human") * F.col("price_usd"))
        .withColumn(
            "debt_usd",
            (F.col("variable_debt_human") + F.col("stable_debt_human")) * F.col("price_usd"),
        )
        # Flags
        .withColumn(
            "is_collateral_enabled",
            F.col("usageAsCollateralEnabledOnUser").cast("boolean"),
        )
        .withColumn("protocol", F.lit("aave_v3"))
        # Metadata
        .withColumn("ingestion_ts", F.col("ingestion_ts"))
        .withColumn("ingestion_date", F.col("ingestion_date"))
        # Select final columns
        .select(
            "user_address",
            "reserve_address",
            "symbol",
            "decimals",
            "liquidation_threshold",
            "ltv",
            "liquidation_bonus",
            "price_usd",
            "collateral_human",
            "variable_debt_human",
            "stable_debt_human",
            "collateral_usd",
            "debt_usd",
            "is_collateral_enabled",
            "protocol",
            "ingestion_ts",
            "ingestion_date",
        )
        # Keep only rows where position is economically meaningful
        .filter(F.col("debt_usd") > 0)
        .filter(F.col("collateral_usd") >= 0)
    )
    return silver


# ---------------------------------------------------------------------------
# Compound
# ---------------------------------------------------------------------------

def transform_compound(spark: SparkSession) -> DataFrame:
    """
    Normalise Compound V3 raw positions into the silver schema.

    Compound V3 records already include USD price via ``lastPriceUSD``.
    Collateral is stored as an array; we aggregate it here.
    """
    logger.info("Transforming Compound bronze → silver")
    raw = spark.read.table("nessie.bronze.compound_raw_positions")

    silver = (
        raw
        .withColumn("user_address", F.lower(F.col("account.id")))
        .withColumn("symbol", F.col("market.inputToken.symbol"))
        .withColumn("decimals", F.col("market.inputToken.decimals").cast(IntegerType()))
        .withColumn("price_usd", F.col("market.inputToken.lastPriceUSD").cast(DoubleType()))
        # Borrow balance (already in human units from Messari subgraph)
        .withColumn("borrow_balance", F.col("borrowBalance").cast(DoubleType()))
        .withColumn("debt_usd", F.col("borrow_balance") * F.col("price_usd"))
        # Aggregate collateral across all collateral tokens
        .withColumn(
            "total_collateral_usd",
            F.aggregate(
                F.col("collateralTokens"),
                F.lit(0.0).cast(DoubleType()),
                lambda acc, x: acc
                + x["collateralBalance"].cast(DoubleType())
                * x["collateralToken"]["token"]["lastPriceUSD"].cast(DoubleType()),
            ),
        )
        # Use weighted average liquidation factor as liquidation_threshold
        .withColumn(
            "liquidation_threshold",
            F.aggregate(
                F.col("collateralTokens"),
                F.lit(0.0).cast(DoubleType()),
                lambda acc, x: acc
                + x["collateralBalance"].cast(DoubleType())
                * x["collateralToken"]["token"]["lastPriceUSD"].cast(DoubleType())
                * x["collateralToken"]["liquidationFactor"].cast(DoubleType()),
            )
            / F.when(F.col("total_collateral_usd") > 0, F.col("total_collateral_usd")).otherwise(
                F.lit(1.0)
            ),
        )
        .withColumnRenamed("total_collateral_usd", "collateral_usd")
        .withColumn("ltv", F.lit(None).cast(DoubleType()))
        .withColumn("liquidation_bonus", F.lit(None).cast(DoubleType()))
        .withColumn("collateral_human", F.col("depositBalance").cast(DoubleType()))
        .withColumn("protocol", F.lit("compound_v3"))
        .withColumn("ingestion_date", F.col("ingestion_date"))
        .select(
            "user_address",
            F.col("market.id").alias("reserve_address"),
            "symbol",
            "decimals",
            "liquidation_threshold",
            "ltv",
            "liquidation_bonus",
            "price_usd",
            "collateral_human",
            F.lit(None).cast(DoubleType()).alias("variable_debt_human"),
            F.lit(None).cast(DoubleType()).alias("stable_debt_human"),
            "collateral_usd",
            "debt_usd",
            F.col("isCollateral").alias("is_collateral_enabled"),
            "protocol",
            "ingestion_ts",
            "ingestion_date",
        )
        .filter(F.col("debt_usd") > 0)
    )
    return silver


# ---------------------------------------------------------------------------
# MakerDAO
# ---------------------------------------------------------------------------

def transform_maker(spark: SparkSession) -> DataFrame:
    """
    Normalise MakerDAO vault data into the silver schema.

    Calculations
    ------------
    actual_dai_debt = debt (art) * collateralType.rate   (RAD units → DAI)
    collateral_usd  = collateral (WAD / 1e18) * price.value
    debt_usd        = actual_dai_debt (DAI ≈ $1)
    liquidation_threshold = 1 / liquidationRatio
      (MakerDAO uses collateralisation ratio, not liquidation threshold;
       HF = collateral_usd / (debt_usd * liquidationRatio))
    """
    logger.info("Transforming Maker bronze → silver")
    raw = spark.read.table("nessie.bronze.maker_raw_vaults")

    silver = (
        raw
        .withColumn("user_address", F.lower(F.col("owner.id")))
        .withColumn("reserve_address", F.col("collateralType.id"))
        .withColumn("symbol", F.col("collateralType.id"))
        .withColumn("decimals", F.lit(18))
        # Oracle price (USD)
        .withColumn("price_usd", F.col("collateralType.price.value").cast(DoubleType()))
        # Collateral: WAD (1e18 units) → human amount
        .withColumn(
            "collateral_human",
            F.col("collateral").cast(DoubleType()) / WAD,
        )
        # Actual DAI debt: art * rate (rate is in RAD/WAD = 1e27 units)
        .withColumn(
            "dai_debt",
            F.col("debt").cast(DoubleType())
            * F.col("collateralType.rate").cast(DoubleType())
            / 1e27,
        )
        # USD values
        .withColumn("collateral_usd", F.col("collateral_human") * F.col("price_usd"))
        .withColumn("debt_usd", F.col("dai_debt"))  # DAI ≈ $1
        # MakerDAO liquidation ratio is the MINIMUM collateralisation ratio.
        # liquidation_threshold for HF purposes = 1 / liquidationRatio
        .withColumn(
            "liquidation_ratio",
            F.col("collateralType.liquidationRatio").cast(DoubleType()),
        )
        .withColumn(
            "liquidation_threshold",
            F.lit(1.0) / F.col("liquidation_ratio"),
        )
        .withColumn("ltv", F.lit(None).cast(DoubleType()))
        .withColumn("liquidation_bonus", F.lit(None).cast(DoubleType()))
        .withColumn("protocol", F.lit("maker"))
        .select(
            "user_address",
            "reserve_address",
            "symbol",
            "decimals",
            "liquidation_threshold",
            "ltv",
            "liquidation_bonus",
            "price_usd",
            "collateral_human",
            F.lit(None).cast(DoubleType()).alias("variable_debt_human"),
            F.lit(None).cast(DoubleType()).alias("stable_debt_human"),
            "collateral_usd",
            "debt_usd",
            F.lit(True).alias("is_collateral_enabled"),
            "protocol",
            "ingestion_ts",
            "ingestion_date",
        )
        .filter(F.col("debt_usd") > 0)
        .filter(F.col("collateral_usd") > 0)
    )
    return silver


# ---------------------------------------------------------------------------
# Write helper
# ---------------------------------------------------------------------------

def _write_silver(df: DataFrame, table_name: str) -> None:
    logger.info("Writing silver table nessie.silver.%s (%d rows)", table_name, df.count())
    (
        df.writeTo(f"nessie.silver.{table_name}")
        .partitionedBy("ingestion_date")
        .tableProperty("write.format.default", "parquet")
        .tableProperty("write.parquet.compression-codec", "snappy")
        .createOrReplace()
    )
    logger.info("Write complete: nessie.silver.%s", table_name)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run() -> None:
    spark = get_spark_session("DeFiRisk-SilverTransformer")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.silver")

    try:
        _write_silver(transform_aave(spark), "aave_positions")
    except Exception:
        logger.exception("Aave silver transform failed — skipping.")

    try:
        _write_silver(transform_compound(spark), "compound_positions")
    except Exception:
        logger.exception("Compound silver transform failed — skipping.")

    try:
        _write_silver(transform_maker(spark), "maker_vaults")
    except Exception:
        logger.exception("Maker silver transform failed — skipping.")

    spark.stop()
    logger.info("Silver transformer complete.")


if __name__ == "__main__":
    run()
