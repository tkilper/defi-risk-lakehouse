"""
Spark Silver Position Snapshots
=================================
Reads all three silver position tables and unions them into a single
timestamped snapshot table used for time-series feature engineering.

Source tables:
  - nessie.silver.aave_positions
  - nessie.silver.compound_positions
  - nessie.silver.maker_vaults

Output:
  - nessie.silver.position_snapshots

Schema:
  snapshot_id          string   (user_address + protocol + snapshot_time hash)
  user_address         string
  protocol             string
  snapshot_time        timestamp
  health_factor        double
  collateral_usd       double
  debt_usd             double
  ltv_ratio            double   (debt_usd / collateral_usd)
  liquidation_threshold double
  symbol               string
  ingestion_date       date

The table is partitioned by ingestion_date and appended to on each run.
Existing rows for the same snapshot_time are replaced (createOrReplace per
partition) to allow idempotent re-runs.

Run:
    spark-submit --master spark://spark-master:7077 silver_position_snapshots.py
"""

from __future__ import annotations

import logging
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

sys.path.insert(0, "/opt/spark/jobs")
from utils import get_spark_session

logging.basicConfig(level=logging.INFO, stream=sys.stdout)
logger = logging.getLogger(__name__)

# Minimum liquidation threshold required to compute health factor.
# Positions with zero threshold are excluded (they cannot be liquidated).
_MIN_LIQ_THRESHOLD = 0.01

# Aave health factor formula:
# HF = (collateral_usd * liquidation_threshold) / debt_usd
# Values below 1.0 are eligible for liquidation.
_DEFAULT_HEALTH_FACTOR = 999.0  # sentinel for zero-debt positions


def _compute_health_factor(collateral_usd, debt_usd, liquidation_threshold):
    """Return a Spark column expression for the health factor."""
    safe_threshold = F.when(
        liquidation_threshold.isNull() | (liquidation_threshold <= 0),
        F.lit(0.8),  # industry-standard default when not available
    ).otherwise(liquidation_threshold)

    return F.when(
        debt_usd.isNull() | (debt_usd <= 0),
        F.lit(_DEFAULT_HEALTH_FACTOR),
    ).otherwise(
        (collateral_usd.cast(DoubleType()) * safe_threshold.cast(DoubleType()))
        / debt_usd.cast(DoubleType())
    )


def _read_and_normalize(spark: SparkSession, table: str, protocol: str) -> DataFrame:
    """
    Read a silver position table and normalize it to the snapshot schema.
    """
    logger.info("Reading %s for snapshot...", table)
    raw = spark.read.table(table)

    collateral = F.col("collateral_usd").cast(DoubleType())
    debt = F.col("debt_usd").cast(DoubleType())
    liq_thresh = F.col("liquidation_threshold").cast(DoubleType())
    ltv_ratio = F.when(collateral > 0, debt / collateral).otherwise(F.lit(None).cast(DoubleType()))

    return (
        raw.withColumn("snapshot_time", F.col("ingestion_ts"))
        .withColumn(
            "health_factor",
            _compute_health_factor(collateral, debt, liq_thresh),
        )
        .withColumn("ltv_ratio", ltv_ratio)
        .withColumn(
            "snapshot_id",
            F.md5(
                F.concat_ws(
                    "|",
                    F.col("user_address"),
                    F.lit(protocol),
                    F.col("snapshot_time").cast("string"),
                )
            ),
        )
        .select(
            "snapshot_id",
            "user_address",
            F.lit(protocol).alias("protocol"),
            "snapshot_time",
            "health_factor",
            collateral.alias("collateral_usd"),
            debt.alias("debt_usd"),
            "ltv_ratio",
            liq_thresh.alias("liquidation_threshold"),
            F.col("symbol"),
            F.col("ingestion_date"),
        )
        .filter(debt > 0)
        .filter(collateral > 0)
    )


def run() -> None:
    spark = get_spark_session("DeFiRisk-SilverPositionSnapshots")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.silver")

    frames: list[DataFrame] = []

    for table, protocol in [
        ("nessie.silver.aave_positions", "aave_v3"),
        ("nessie.silver.compound_positions", "compound_v3"),
        ("nessie.silver.maker_vaults", "maker"),
    ]:
        try:
            frames.append(_read_and_normalize(spark, table, protocol))
        except Exception:
            logger.exception("Failed to read %s for snapshots — skipping.", table)

    if not frames:
        logger.warning("No position data available — snapshot table not written.")
        spark.stop()
        return

    combined = frames[0]
    for frame in frames[1:]:
        combined = combined.union(frame)

    row_count = combined.count()
    logger.info("Writing %d position snapshots to nessie.silver.position_snapshots", row_count)

    (
        combined.writeTo("nessie.silver.position_snapshots")
        .partitionedBy("ingestion_date")
        .tableProperty("write.format.default", "parquet")
        .tableProperty("write.parquet.compression-codec", "snappy")
        .createOrReplace()
    )
    logger.info("Write complete: nessie.silver.position_snapshots")
    spark.stop()


if __name__ == "__main__":
    run()
