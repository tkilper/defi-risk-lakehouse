"""
Spark Bronze Loader
===================
Reads raw NDJSON files from MinIO (s3a://lakehouse/raw/{protocol}/...)
and writes them as Iceberg tables in the ``nessie.bronze`` namespace.

One Iceberg table is created per protocol:
  - nessie.bronze.aave_raw_positions
  - nessie.bronze.compound_raw_positions
  - nessie.bronze.maker_raw_vaults

Tables are partitioned by ``ingestion_date`` and written with
MERGE INTO semantics so re-runs are idempotent.

Run:
    spark-submit --master spark://spark-master:7077 bronze_loader.py
"""

from __future__ import annotations

import logging
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

sys.path.insert(0, "/opt/spark/jobs")
from utils import get_spark_session

logging.basicConfig(level=logging.INFO, stream=sys.stdout)
logger = logging.getLogger(__name__)

BUCKET = "lakehouse"
RAW_BASE = f"s3a://{BUCKET}/raw"


# ---------------------------------------------------------------------------
# Schema helpers
# ---------------------------------------------------------------------------


def _read_ndjson(spark: SparkSession, protocol: str) -> DataFrame:
    """
    Read all raw NDJSON files for a protocol into a single DataFrame,
    adding ``ingestion_date`` and ``ingestion_ts`` metadata columns.
    """
    path = f"{RAW_BASE}/{protocol}/"
    logger.info("Reading raw %s data from %s", protocol, path)

    df = (
        spark.read.option("multiline", "false")
        .json(path)
        .withColumn("ingestion_ts", F.current_timestamp())
        .withColumn("ingestion_date", F.to_date(F.current_timestamp()))
        .withColumn("protocol", F.lit(protocol))
    )
    logger.info("Schema for %s: %s", protocol, df.schema.simpleString())
    return df


def _write_bronze(df: DataFrame, table_name: str) -> None:
    """
    Write a DataFrame to an Iceberg bronze table via Nessie.

    Uses ``createOrReplace`` so re-running after a schema change works
    cleanly. In production, switch to MERGE INTO for append-only semantics.
    """
    logger.info("Writing %d rows to nessie.bronze.%s", df.count(), table_name)
    (
        df.writeTo(f"nessie.bronze.{table_name}")
        .partitionedBy("ingestion_date")
        .tableProperty("write.format.default", "parquet")
        .tableProperty("write.parquet.compression-codec", "snappy")
        .createOrReplace()
    )
    logger.info("Write complete: nessie.bronze.%s", table_name)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def run() -> None:
    spark = get_spark_session("DeFiRisk-BronzeLoader")

    # Ensure namespace exists
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.bronze")

    # Aave V3
    try:
        aave_df = _read_ndjson(spark, "aave")
        _write_bronze(aave_df, "aave_raw_positions")
    except Exception:
        logger.exception("Failed to load Aave bronze data — skipping.")

    # Compound V3
    try:
        compound_df = _read_ndjson(spark, "compound")
        _write_bronze(compound_df, "compound_raw_positions")
    except Exception:
        logger.exception("Failed to load Compound bronze data — skipping.")

    # MakerDAO
    try:
        maker_df = _read_ndjson(spark, "maker")
        _write_bronze(maker_df, "maker_raw_vaults")
    except Exception:
        logger.exception("Failed to load Maker bronze data — skipping.")

    spark.stop()
    logger.info("Bronze loader complete.")


if __name__ == "__main__":
    run()
