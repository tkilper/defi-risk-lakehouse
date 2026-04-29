"""
Spark Feature Pipeline
=======================
Builds the ML feature table (nessie.gold.ml_features) by:

1. Reading nessie.silver.position_snapshots (hourly position state)
2. Reading nessie.silver.liquidation_events (ground truth)
3. Engineering time-series features via Spark window functions
4. Joining features to 24h liquidation labels
5. Writing nessie.gold.ml_features partitioned by snapshot_date

Key design decisions
--------------------
- Only snapshots where 24h has elapsed since snapshot_time receive a label.
  Snapshots from the last 24h are excluded from training (they may still liquidate).
- Time-series cross-validation requires a snapshot_date partition so the
  training loop can select clean train/validation splits.
- Price velocity features use approximate ETH/BTC prices derived from
  WETH/WBTC collateral positions as a proxy (no external price API required).

Run:
    spark-submit --master spark://spark-master:7077 feature_pipeline.py
    (via the feature_engineering_dag Airflow DAG)
"""

from __future__ import annotations

import logging
import sys
from datetime import UTC, datetime

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, LongType

sys.path.insert(0, "/opt/spark/jobs")
sys.path.insert(0, "/opt/airflow")
from utils import get_spark_session

logging.basicConfig(level=logging.INFO, stream=sys.stdout)
logger = logging.getLogger(__name__)

# Exclude snapshots from the last 24h — they may still liquidate before label cut-off
_LABEL_CUTOFF_SECONDS = 24 * 3600

# ETH is considered dominant if it makes up > 60% of collateral (by USD value)
_ETH_DOMINANT_THRESHOLD = 0.60

# Protocol encoding
_PROTOCOL_MAP: dict[str, int] = {"aave_v3": 0, "compound_v3": 1, "maker": 2}


# ---------------------------------------------------------------------------
# 1. Load base data
# ---------------------------------------------------------------------------


def load_snapshots(spark: SparkSession) -> DataFrame:
    logger.info("Loading position snapshots...")
    return spark.read.table("nessie.silver.position_snapshots")


def load_liquidations(spark: SparkSession) -> DataFrame:
    logger.info("Loading liquidation events...")
    return spark.read.table("nessie.silver.liquidation_events")


# ---------------------------------------------------------------------------
# 2. Build ground truth labels
# ---------------------------------------------------------------------------


def build_labels(snapshots: DataFrame, liquidations: DataFrame) -> DataFrame:
    """
    For each snapshot, determine if the position was liquidated within 24h.

    Join condition:
      liquidation.user_address == snapshot.user_address
      AND liquidation.protocol == snapshot.protocol
      AND liquidation.timestamp BETWEEN snapshot_ts AND snapshot_ts + 24h

    Snapshots within 24h of NOW are excluded (label would be premature).
    """
    now_ts = int(datetime.now(tz=UTC).timestamp())
    cutoff_ts = now_ts - _LABEL_CUTOFF_SECONDS

    # Filter out snapshots too recent to have a definitive label
    snaps = snapshots.filter(
        F.col("snapshot_time").cast(LongType()) <= cutoff_ts
    )

    liq_events = liquidations.select(
        F.col("user_address").alias("liq_user"),
        F.col("protocol").alias("liq_protocol"),
        F.col("timestamp").alias("liq_ts"),
    )

    # Broadcast the liquidation events side (typically much smaller)
    labeled = (
        snaps.join(
            F.broadcast(liq_events),
            on=(
                (snaps["user_address"] == liq_events["liq_user"])
                & (snaps["protocol"] == liq_events["liq_protocol"])
                & (
                    liq_events["liq_ts"].cast(LongType())
                    >= snaps["snapshot_time"].cast(LongType())
                )
                & (
                    liq_events["liq_ts"].cast(LongType())
                    <= snaps["snapshot_time"].cast(LongType()) + _LABEL_CUTOFF_SECONDS
                )
            ),
            how="left",
        )
        .withColumn(
            "liquidated_within_24h",
            F.when(F.col("liq_ts").isNotNull(), F.lit(1)).otherwise(F.lit(0)),
        )
        .drop("liq_user", "liq_protocol", "liq_ts")
        .dropDuplicates(["snapshot_id"])  # a position may have multiple liq events
    )

    return labeled


# ---------------------------------------------------------------------------
# 3. Time-series features using window functions
# ---------------------------------------------------------------------------


def add_health_factor_trends(df: DataFrame) -> DataFrame:
    """
    Compute HF delta over 6h and 24h windows, and the 24h minimum HF.

    Uses a row-based window ordered by snapshot_time within each user+protocol group.
    """
    user_time_window = Window.partitionBy("user_address", "protocol").orderBy(
        F.col("snapshot_time").cast(LongType())
    )

    # Approximate 6h and 24h lookback using rangeBetween (seconds)
    window_6h = Window.partitionBy("user_address", "protocol").orderBy(
        F.col("snapshot_time").cast(LongType())
    ).rangeBetween(-6 * 3600, 0)

    window_24h = Window.partitionBy("user_address", "protocol").orderBy(
        F.col("snapshot_time").cast(LongType())
    ).rangeBetween(-24 * 3600, 0)

    hf_6h_ago = F.first("health_factor").over(window_6h)
    hf_24h_ago = F.first("health_factor").over(window_24h)

    return (
        df.withColumn(
            "health_factor_delta_6h",
            F.col("health_factor") - hf_6h_ago,
        )
        .withColumn(
            "health_factor_delta_24h",
            F.col("health_factor") - hf_24h_ago,
        )
        .withColumn(
            "health_factor_min_24h",
            F.min("health_factor").over(window_24h),
        )
    )


def add_price_velocity_features(df: DataFrame) -> DataFrame:
    """
    Approximate ETH price velocity from WETH collateral USD per token.

    Since we don't have a separate price oracle table, we proxy ETH price
    using price_usd of WETH positions. When WETH positions aren't present
    for a user, we fall back to null (XGBoost handles nulls natively).
    """
    # For positions using ETH/WETH collateral, the price_usd column reflects
    # the oracle price at ingestion time. We approximate velocity using
    # the per-user collateral_usd change relative to debt (indirect proxy).
    # A dedicated price oracle table would give cleaner signals here.

    window_6h = Window.partitionBy("user_address", "protocol").orderBy(
        F.col("snapshot_time").cast(LongType())
    ).rangeBetween(-6 * 3600, 0)

    window_24h = Window.partitionBy("user_address", "protocol").orderBy(
        F.col("snapshot_time").cast(LongType())
    ).rangeBetween(-24 * 3600, 0)

    # Proxy: collateral_usd change as a proxy for price movement
    prev_collateral_6h = F.first("collateral_usd").over(window_6h)
    prev_collateral_24h = F.first("collateral_usd").over(window_24h)

    return (
        df.withColumn(
            "eth_price_change_pct_1h",
            F.lit(None).cast(DoubleType()),  # requires 1h snapshots; null for now
        )
        .withColumn(
            "eth_price_change_pct_6h",
            F.when(
                prev_collateral_6h > 0,
                (F.col("collateral_usd") - prev_collateral_6h) / prev_collateral_6h * 100.0,
            ).otherwise(F.lit(None).cast(DoubleType())),
        )
        .withColumn(
            "eth_price_change_pct_24h",
            F.when(
                prev_collateral_24h > 0,
                (F.col("collateral_usd") - prev_collateral_24h) / prev_collateral_24h * 100.0,
            ).otherwise(F.lit(None).cast(DoubleType())),
        )
        .withColumn(
            "btc_price_change_pct_24h",
            F.lit(None).cast(DoubleType()),  # requires BTC price oracle; null for now
        )
    )


def add_position_structure_features(df: DataFrame) -> DataFrame:
    """
    Add collateral concentration and diversification features.

    With a single-reserve-per-row schema (current silver), we treat each row
    as a single-asset position. Aggregating across multiple reserves for the
    same user would require a join/agg step not yet available without a
    dedicated position schema. Documented as a future improvement.
    """
    return (
        df.withColumn("num_collateral_assets", F.lit(1).cast(IntegerType()))
        .withColumn("largest_collateral_pct", F.lit(1.0).cast(DoubleType()))
        .withColumn(
            "is_eth_dominant_collateral",
            F.col("symbol").isin("WETH", "ETH", "wstETH", "cbETH").cast(IntegerType()),
        )
        .withColumn(
            "collateral_volatility_30d",
            F.lit(None).cast(DoubleType()),  # requires 30d price history
        )
    )


def add_protocol_features(df: DataFrame) -> DataFrame:
    """Label-encode the protocol and add borrow_apy placeholder."""
    protocol_map = F.create_map(
        F.lit("aave_v3"), F.lit(0),
        F.lit("compound_v3"), F.lit(1),
        F.lit("maker"), F.lit(2),
    )
    return (
        df.withColumn(
            "protocol_encoded",
            F.coalesce(protocol_map[F.col("protocol")], F.lit(-1)).cast(IntegerType()),
        )
        .withColumn(
            "borrow_apy",
            F.lit(None).cast(DoubleType()),  # available in bronze; add in future pass
        )
    )


def add_user_history_features(df: DataFrame, liquidations: DataFrame) -> DataFrame:
    """
    Count prior liquidations per user and compute days since first position.
    """
    prior_liq_counts = (
        liquidations.groupBy("user_address")
        .agg(F.count("*").alias("user_prior_liquidations"))
    )

    first_seen = (
        df.groupBy("user_address", "protocol")
        .agg(F.min("snapshot_time").alias("first_snapshot_time"))
    )

    return (
        df.join(prior_liq_counts, on="user_address", how="left")
        .join(first_seen, on=["user_address", "protocol"], how="left")
        .withColumn(
            "user_prior_liquidations",
            F.coalesce(F.col("user_prior_liquidations"), F.lit(0)).cast(IntegerType()),
        )
        .withColumn(
            "days_position_open",
            (
                (F.col("snapshot_time").cast(LongType()) - F.col("first_snapshot_time").cast(LongType()))
                / 86_400.0
            ).cast(DoubleType()),
        )
        .drop("first_snapshot_time")
    )


# ---------------------------------------------------------------------------
# 4. Assemble final feature table
# ---------------------------------------------------------------------------


def build_feature_table(spark: SparkSession) -> DataFrame:
    snapshots = load_snapshots(spark)
    liquidations = load_liquidations(spark)

    logger.info("Building ground truth labels...")
    labeled = build_labels(snapshots, liquidations)

    logger.info("Engineering health factor trend features...")
    labeled = add_health_factor_trends(labeled)

    logger.info("Engineering price velocity features...")
    labeled = add_price_velocity_features(labeled)

    logger.info("Engineering position structure features...")
    labeled = add_position_structure_features(labeled)

    logger.info("Engineering protocol features...")
    labeled = add_protocol_features(labeled)

    logger.info("Engineering user history features...")
    labeled = add_user_history_features(labeled, liquidations)

    # Add partition column
    labeled = labeled.withColumn(
        "snapshot_date", F.to_date(F.col("snapshot_time"))
    )

    feature_cols = [
        "snapshot_id",
        "user_address",
        "protocol",
        "snapshot_time",
        "snapshot_date",
        # Base features
        "health_factor",
        "collateral_usd",
        "debt_usd",
        "ltv_ratio",
        "liquidation_threshold",
        # Price velocity
        "eth_price_change_pct_1h",
        "eth_price_change_pct_6h",
        "eth_price_change_pct_24h",
        "btc_price_change_pct_24h",
        # HF trend
        "health_factor_delta_6h",
        "health_factor_delta_24h",
        "health_factor_min_24h",
        # Structure
        "num_collateral_assets",
        "largest_collateral_pct",
        "is_eth_dominant_collateral",
        "collateral_volatility_30d",
        # Protocol
        "protocol_encoded",
        "borrow_apy",
        # User history
        "user_prior_liquidations",
        "days_position_open",
        # Label
        "liquidated_within_24h",
    ]

    return labeled.select(feature_cols)


def run() -> None:
    spark = get_spark_session("DeFiRisk-FeaturePipeline")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.gold")

    features = build_feature_table(spark)
    row_count = features.count()
    liq_count = features.filter(F.col("liquidated_within_24h") == 1).count()

    logger.info(
        "Feature table: %d rows, %d liquidations (%.1f%% positive rate)",
        row_count,
        liq_count,
        100.0 * liq_count / max(row_count, 1),
    )

    (
        features.writeTo("nessie.gold.ml_features")
        .partitionedBy("snapshot_date")
        .tableProperty("write.format.default", "parquet")
        .tableProperty("write.parquet.compression-codec", "snappy")
        .createOrReplace()
    )

    logger.info("Write complete: nessie.gold.ml_features")
    spark.stop()


if __name__ == "__main__":
    run()
