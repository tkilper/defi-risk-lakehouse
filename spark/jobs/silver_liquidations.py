"""
Spark Silver Liquidations Transformer
======================================
Reads bronze liquidation tables from ``nessie.bronze`` and produces a
unified, protocol-normalised silver table:

  - nessie.silver.liquidation_events

Schema:
  liquidation_id       string   (source event ID)
  protocol             string   (aave_v3 | compound_v3 | maker)
  user_address         string   (liquidated position owner)
  liquidator_address   string
  collateral_asset     string   (symbol of collateral seized)
  debt_asset           string   (symbol of debt repaid)
  collateral_amount_usd  double
  debt_covered_usd       double
  timestamp            long     (Unix epoch seconds)
  block_number         long
  ingestion_ts         timestamp
  ingestion_date       date

Run:
    spark-submit --master spark://spark-master:7077 silver_liquidations.py
"""

from __future__ import annotations

import logging
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType

sys.path.insert(0, "/opt/spark/jobs")
from utils import get_spark_session

logging.basicConfig(level=logging.INFO, stream=sys.stdout)
logger = logging.getLogger(__name__)

# Hard-coded approximate ETH/USD — same fallback used in silver_transformer.py
_FALLBACK_ETH_USD = 3_000.0

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
}


def _fallback_price_map(spark: SparkSession):
    return F.create_map(
        *[
            item
            for pair in [(F.lit(k), F.lit(v)) for k, v in _FALLBACK_PRICES_USD.items()]
            for item in pair
        ]
    )


# ---------------------------------------------------------------------------
# Aave V3
# ---------------------------------------------------------------------------


def transform_aave(spark: SparkSession) -> DataFrame:
    """
    Normalise Aave V3 liquidation events.

    Amounts in the subgraph are in raw token units (scaled by 10^decimals).
    We convert to USD using the same ETH-price fallback as silver_transformer.
    """
    logger.info("Transforming Aave bronze liquidations → silver")
    raw = spark.read.table("nessie.bronze.aave_liquidations_raw")
    fallback = _fallback_price_map(spark)

    # Aave subgraph amounts are in raw token units; decimals are available on reserves
    collateral_decimals = F.col("collateralReserve.decimals").cast(DoubleType())
    debt_decimals = F.col("principalReserve.decimals").cast(DoubleType())

    collateral_amount = F.col("collateralAmount").cast(DoubleType()) / F.pow(
        F.lit(10.0), collateral_decimals
    )
    debt_amount = F.col("principalAmount").cast(DoubleType()) / F.pow(
        F.lit(10.0), debt_decimals
    )

    collateral_symbol = F.col("collateralReserve.symbol")
    debt_symbol = F.col("principalReserve.symbol")

    collateral_price = F.coalesce(fallback[collateral_symbol], F.lit(_FALLBACK_ETH_USD))
    debt_price = F.coalesce(fallback[debt_symbol], F.lit(1.0))

    return (
        raw.select(
            F.col("id").alias("liquidation_id"),
            F.lit("aave_v3").alias("protocol"),
            F.lower(F.col("user.id")).alias("user_address"),
            F.lower(F.col("liquidator.id")).alias("liquidator_address"),
            collateral_symbol.alias("collateral_asset"),
            debt_symbol.alias("debt_asset"),
            (collateral_amount * collateral_price).alias("collateral_amount_usd"),
            (debt_amount * debt_price).alias("debt_covered_usd"),
            F.col("timestamp").cast(LongType()).alias("timestamp"),
            F.col("blockNumber").cast(LongType()).alias("block_number"),
            F.col("ingestion_ts"),
            F.col("ingestion_date"),
        )
        .filter(F.col("collateral_amount_usd") > 0)
    )


# ---------------------------------------------------------------------------
# Compound V3 (Messari schema)
# ---------------------------------------------------------------------------


def transform_compound(spark: SparkSession) -> DataFrame:
    """
    Normalise Compound V3 liquidation events from the Messari subgraph.

    The Messari schema provides amountUSD directly, which we use when available.
    """
    logger.info("Transforming Compound bronze liquidations → silver")
    raw = spark.read.table("nessie.bronze.compound_liquidations_raw")

    return (
        raw.select(
            F.col("id").alias("liquidation_id"),
            F.lit("compound_v3").alias("protocol"),
            F.lower(F.col("liquidatee")).alias("user_address"),
            F.lower(F.col("liquidator")).alias("liquidator_address"),
            F.col("asset.symbol").alias("collateral_asset"),
            F.lit("USDC").alias("debt_asset"),  # Compound V3 borrows are USDC-denominated
            F.col("amountUSD").cast(DoubleType()).alias("collateral_amount_usd"),
            F.col("amountUSD").cast(DoubleType()).alias("debt_covered_usd"),
            F.col("timestamp").cast(LongType()).alias("timestamp"),
            F.col("blockNumber").cast(LongType()).alias("block_number"),
            F.col("ingestion_ts"),
            F.col("ingestion_date"),
        )
        .filter(F.col("collateral_amount_usd") > 0)
    )


# ---------------------------------------------------------------------------
# MakerDAO (Messari schema)
# ---------------------------------------------------------------------------


def transform_maker(spark: SparkSession) -> DataFrame:
    """
    Normalise MakerDAO Bite events from the Messari subgraph.

    Debt is always DAI ≈ $1, so debt_covered_usd ≈ amount * price.
    """
    logger.info("Transforming Maker bronze liquidations → silver")
    raw = spark.read.table("nessie.bronze.maker_liquidations_raw")

    return (
        raw.select(
            F.col("id").alias("liquidation_id"),
            F.lit("maker").alias("protocol"),
            F.lower(F.col("liquidatee")).alias("user_address"),
            F.lower(F.col("liquidator")).alias("liquidator_address"),
            F.col("asset.symbol").alias("collateral_asset"),
            F.lit("DAI").alias("debt_asset"),
            F.col("amountUSD").cast(DoubleType()).alias("collateral_amount_usd"),
            F.col("amountUSD").cast(DoubleType()).alias("debt_covered_usd"),
            F.col("timestamp").cast(LongType()).alias("timestamp"),
            F.col("blockNumber").cast(LongType()).alias("block_number"),
            F.col("ingestion_ts"),
            F.col("ingestion_date"),
        )
        .filter(F.col("collateral_amount_usd") > 0)
    )


# ---------------------------------------------------------------------------
# Write helper
# ---------------------------------------------------------------------------


def _write_silver(df: DataFrame, table_name: str) -> None:
    logger.info("Writing %d rows to nessie.silver.%s", df.count(), table_name)
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
    spark = get_spark_session("DeFiRisk-SilverLiquidations")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.silver")

    frames = []

    for transform_fn, protocol_label in [
        (transform_aave, "aave"),
        (transform_compound, "compound"),
        (transform_maker, "maker"),
    ]:
        try:
            frames.append(transform_fn(spark))
        except Exception:
            logger.exception("Failed to transform %s liquidations — skipping.", protocol_label)

    if frames:
        combined = frames[0]
        for frame in frames[1:]:
            combined = combined.union(frame)
        _write_silver(combined, "liquidation_events")
    else:
        logger.warning("No liquidation data transformed — silver table not written.")

    spark.stop()
    logger.info("Silver liquidations transformer complete.")


if __name__ == "__main__":
    run()
