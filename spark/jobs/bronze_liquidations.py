"""
Spark Bronze Liquidations Loader
=================================
Reads raw liquidation NDJSON files from MinIO
  s3a://lakehouse/raw/liquidations/{protocol}/
and writes them as Iceberg tables in ``nessie.bronze``:

  - nessie.bronze.aave_liquidations_raw
  - nessie.bronze.compound_liquidations_raw
  - nessie.bronze.maker_liquidations_raw

Run:
    spark-submit --master spark://spark-master:7077 bronze_liquidations.py
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
RAW_BASE = f"s3a://{BUCKET}/raw/liquidations"


def _read_ndjson(spark: SparkSession, protocol: str) -> DataFrame:
    path = f"{RAW_BASE}/{protocol}/"
    logger.info("Reading raw liquidation data for %s from %s", protocol, path)
    return (
        spark.read.option("multiline", "false")
        .json(path)
        .withColumn("ingestion_ts", F.current_timestamp())
        .withColumn("ingestion_date", F.to_date(F.current_timestamp()))
        .withColumn("protocol", F.lit(protocol))
    )


def _write_bronze(df: DataFrame, table_name: str) -> None:
    logger.info("Writing %d rows to nessie.bronze.%s", df.count(), table_name)
    (
        df.writeTo(f"nessie.bronze.{table_name}")
        .partitionedBy("ingestion_date")
        .tableProperty("write.format.default", "parquet")
        .tableProperty("write.parquet.compression-codec", "snappy")
        .createOrReplace()
    )
    logger.info("Write complete: nessie.bronze.%s", table_name)


def run() -> None:
    spark = get_spark_session("DeFiRisk-BronzeLiquidations")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.bronze")

    try:
        df = _read_ndjson(spark, "aave")
        _write_bronze(df, "aave_liquidations_raw")
    except Exception:
        logger.exception("Failed to load Aave liquidation bronze data — skipping.")

    try:
        df = _read_ndjson(spark, "compound")
        _write_bronze(df, "compound_liquidations_raw")
    except Exception:
        logger.exception("Failed to load Compound liquidation bronze data — skipping.")

    try:
        df = _read_ndjson(spark, "maker")
        _write_bronze(df, "maker_liquidations_raw")
    except Exception:
        logger.exception("Failed to load Maker liquidation bronze data — skipping.")

    spark.stop()
    logger.info("Bronze liquidations loader complete.")


if __name__ == "__main__":
    run()
