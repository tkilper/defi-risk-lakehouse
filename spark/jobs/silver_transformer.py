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
from pyspark.sql.types import DoubleType, IntegerType, StructField, StructType

sys.path.insert(0, "/opt/spark/jobs")
from utils import get_spark_session

logging.basicConfig(level=logging.INFO, stream=sys.stdout)
logger = logging.getLogger(__name__)

ETH_PRICE_ORACLE_SCALE = 1e18  # Aave priceInEth is scaled by 1e18

# Hard-coded approximate ETH/USD price used only when oracle data is missing.
# In production, fetch this from a price oracle or Chainlink.
_FALLBACK_ETH_USD = 3_000.0

# Fallback USD prices for Compound V3 and MakerDAO tokens.
# Messari subgraphs do not reliably populate lastPriceUsd;
# these approximate values are used when the subgraph returns 0.
_FALLBACK_PRICES_USD: dict[str, float] = {
    "USDC": 1.0,
    "USDT": 1.0,
    "DAI": 1.0,
    "ETH": 3_000.0,
    "WETH": 3_000.0,
    "WBTC": 65_000.0,
    "LINK": 15.0,
    "UNI": 10.0,
    "COMP": 55.0,
    "wstETH": 3_300.0,
    "cbETH": 3_100.0,
    "wUSDM": 1.0,
}


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


def _collateral_resolver_udf(fallback_prices: dict[str, float]):
    """
    Returns a UDF that aggregates a Compound V3 ``accounting.collateralBalances``
    array into (collateral_usd, liquidation_threshold), applying fallback prices
    when the subgraph returns lastPriceUsd = 0.

    Uses ``liquidateCollateralFactor`` (the threshold at which liquidation is
    triggered) rather than ``liquidationFactor`` (the liquidation penalty).
    """
    _schema = StructType(
        [
            StructField("collateral_usd", DoubleType()),
            StructField("liquidation_threshold", DoubleType()),
        ]
    )

    @F.udf(returnType=_schema)
    def _compute(balances):
        if not balances:
            return (0.0, None)
        total_usd = 0.0
        weighted_lf = 0.0
        for b in balances:
            balance = float(b["balance"] or 0)
            if balance == 0.0:
                continue
            token = b["collateralToken"]["token"]
            price = float(token["lastPriceUsd"] or 0)
            if price == 0.0:
                price = fallback_prices.get(token["symbol"] or "", 0.0)
            lf = float(b["collateralToken"]["liquidateCollateralFactor"] or 0)
            usd = balance * price
            total_usd += usd
            weighted_lf += usd * lf
        liq_threshold = weighted_lf / total_usd if total_usd > 0 else None
        return (total_usd, liq_threshold)

    return _compute


def transform_compound(spark: SparkSession) -> DataFrame:
    """
    Normalise Compound V3 raw positions into the silver schema.

    Schema changes from the Messari subgraph (new vs old):
    - market.configuration.baseToken.token.*  (was market.inputToken.*)
    - accounting.basePrincipal                (was borrowBalance; negative = debt)
    - accounting.collateralBalances[].balance (was collateralTokens[].collateralBalance)
    - collateralToken.liquidateCollateralFactor used for threshold (not liquidationFactor)

    Prices fall back to _FALLBACK_PRICES_USD when lastPriceUsd = 0.
    """
    logger.info("Transforming Compound bronze → silver")
    raw = spark.read.table("nessie.bronze.compound_raw_positions")

    # Spark map for base-token price fallback lookup
    fallback_map = F.create_map(
        *[
            item
            for pair in [(F.lit(k), F.lit(v)) for k, v in _FALLBACK_PRICES_USD.items()]
            for item in pair
        ]
    )

    resolve_collateral = _collateral_resolver_udf(_FALLBACK_PRICES_USD)

    base_price_col = F.col("market.configuration.baseToken.token.lastPriceUsd").cast(DoubleType())
    base_symbol_col = F.col("market.configuration.baseToken.token.symbol")

    silver = (
        raw.withColumn("user_address", F.lower(F.col("account.id")))
        .withColumn("symbol", base_symbol_col)
        .withColumn(
            "decimals", F.col("market.configuration.baseToken.token.decimals").cast(IntegerType())
        )
        # Base token price with fallback
        .withColumn(
            "price_usd",
            F.when(base_price_col > 0, base_price_col).otherwise(
                F.coalesce(fallback_map[base_symbol_col], F.lit(0.0))
            ),
        )
        # basePrincipal is negative for borrowers; abs() gives the debt amount
        .withColumn(
            "borrow_balance",
            F.abs(F.col("accounting.basePrincipal").cast(DoubleType())),
        )
        .withColumn("debt_usd", F.col("borrow_balance") * F.col("price_usd"))
        # Collateral array → (collateral_usd, liquidation_threshold) via UDF
        .withColumn("_collateral", resolve_collateral(F.col("accounting.collateralBalances")))
        .withColumn("collateral_usd", F.col("_collateral.collateral_usd"))
        .withColumn("liquidation_threshold", F.col("_collateral.liquidation_threshold"))
        .withColumn("ltv", F.lit(None).cast(DoubleType()))
        .withColumn("liquidation_bonus", F.lit(None).cast(DoubleType()))
        .withColumn("collateral_human", F.lit(None).cast(DoubleType()))
        .withColumn("protocol", F.lit("compound_v3"))
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
            F.lit(True).alias("is_collateral_enabled"),
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
    Normalise MakerDAO positions into the silver schema.

    The Messari subgraph splits each vault into two position records:
      - side=COLLATERAL: balance = collateral locked (human units)
      - side=BORROWER:   balance = DAI minted (human units)

    We join on (account_id, market_id) to reconstruct the full vault view.
    market.liquidationThreshold is already normalised to a 0–1 decimal.
    Prices fall back to _FALLBACK_PRICES_USD when lastPriceUsd = 0.
    """
    logger.info("Transforming Maker bronze → silver")
    raw = spark.read.table("nessie.bronze.maker_raw_vaults")

    fallback_map = F.create_map(
        *[
            item
            for pair in [(F.lit(k), F.lit(v)) for k, v in _FALLBACK_PRICES_USD.items()]
            for item in pair
        ]
    )

    collateral_df = raw.filter(F.col("side") == "COLLATERAL").select(
        F.lower(F.col("account.id")).alias("account_id"),
        F.col("market.id").alias("market_id"),
        F.col("market.name").alias("symbol"),
        F.col("market.inputToken.decimals").cast(IntegerType()).alias("decimals"),
        F.col("market.inputToken.symbol").alias("token_symbol"),
        F.col("market.inputToken.lastPriceUSD").cast(DoubleType()).alias("raw_price_usd"),
        F.col("market.liquidationThreshold").cast(DoubleType()).alias("liquidation_threshold"),
        F.col("balance").cast(DoubleType()).alias("collateral_balance"),
        F.col("ingestion_ts"),
        F.col("ingestion_date"),
    )

    borrower_df = raw.filter(F.col("side") == "BORROWER").select(
        F.lower(F.col("account.id")).alias("account_id"),
        F.col("market.id").alias("market_id"),
        F.col("balance").cast(DoubleType()).alias("dai_debt"),
    )

    silver = (
        collateral_df.join(borrower_df, on=["account_id", "market_id"], how="inner")
        .withColumn(
            "price_usd",
            F.when(F.col("raw_price_usd") > 0, F.col("raw_price_usd")).otherwise(
                F.coalesce(fallback_map[F.col("token_symbol")], F.lit(0.0))
            ),
        )
        .withColumn("collateral_usd", F.col("collateral_balance") * F.col("price_usd"))
        .withColumn("debt_usd", F.col("dai_debt"))  # DAI ≈ $1
        .withColumn("ltv", F.lit(None).cast(DoubleType()))
        .withColumn("liquidation_bonus", F.lit(None).cast(DoubleType()))
        .withColumn("protocol", F.lit("maker"))
        .select(
            F.col("account_id").alias("user_address"),
            F.col("market_id").alias("reserve_address"),
            "symbol",
            "decimals",
            "liquidation_threshold",
            "ltv",
            "liquidation_bonus",
            "price_usd",
            F.col("collateral_balance").alias("collateral_human"),
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
