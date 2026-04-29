"""
Data + Prediction Drift Monitoring with Evidently AI
======================================================
Compares:
  - Reference dataset: feature table used to train the current Production model
  - Current dataset:   last 7 days of incoming position snapshots

Two reports are generated and logged to MLflow:
  1. Data drift report  — feature distribution shifts
  2. Prediction drift   — shifts in the model's output distribution

Reports are also saved as HTML files in reports/.

Usage:
    python monitoring/drift_report.py
    python monitoring/drift_report.py --lookback-days 7

Drift detection thresholds:
    If > 25% of features show statistical drift → flag for retraining.
    If mean prediction probability shifts by > 0.10 → flag for investigation.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import mlflow
import pandas as pd
import trino
from evidently import ColumnMapping
from evidently.metric_preset import DataDriftPreset
from evidently.metrics import (
    ColumnDriftMetric,
    DatasetDriftMetric,
)
from evidently.report import Report

sys.path.insert(0, str(Path(__file__).parents[1]))
from features.feature_definitions import ALL_FEATURES, LABEL_COL

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
TRINO_HOST = os.getenv("TRINO_HOST", "localhost")
TRINO_PORT = int(os.getenv("TRINO_PORT", "8081"))
REPORTS_DIR = Path("reports")

# Drift threshold: fraction of drifted features that triggers a retrain flag
DRIFT_THRESHOLD = 0.25


def _trino_conn():
    return trino.dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user="monitoring",
        catalog="lakehouse",
        schema="gold",
    )


def load_reference_data(lookback_days: int = 90) -> pd.DataFrame:
    """Load the feature distribution used to train the current Production model."""
    cutoff = (datetime.now(tz=UTC) - timedelta(days=lookback_days)).date()
    conn = _trino_conn()
    query = f"""
        SELECT {', '.join(ALL_FEATURES)}, {LABEL_COL}
        FROM ml_features
        WHERE snapshot_date < DATE '{cutoff}'
        ORDER BY snapshot_date DESC
        LIMIT 50000
    """
    df = pd.read_sql(query, conn)
    logger.info("Reference data: %d rows (snapshots before %s)", len(df), cutoff)
    return df


def load_current_data(lookback_days: int = 7) -> pd.DataFrame:
    """Load the most recent N days of position snapshots (current drift window)."""
    cutoff = (datetime.now(tz=UTC) - timedelta(days=lookback_days)).date()
    conn = _trino_conn()
    query = f"""
        SELECT {', '.join(ALL_FEATURES)}, {LABEL_COL}
        FROM ml_features
        WHERE snapshot_date >= DATE '{cutoff}'
        ORDER BY snapshot_date ASC
    """
    df = pd.read_sql(query, conn)
    logger.info("Current data: %d rows (last %d days)", len(df), lookback_days)
    return df


def load_prediction_data(lookback_days: int = 7) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Load this week's and last week's prediction probability distributions
    from the predictions_log table in Postgres.
    """
    import psycopg2

    pg_host = os.getenv("POSTGRES_HOST", "postgres")
    pg_db = os.getenv("POSTGRES_DB", "airflow")
    pg_user = os.getenv("POSTGRES_USER", "airflow")
    pg_password = os.getenv("POSTGRES_PASSWORD", "airflow")

    conn = psycopg2.connect(
        host=pg_host, dbname=pg_db, user=pg_user, password=pg_password
    )
    now = datetime.now(tz=UTC)
    this_week_start = (now - timedelta(days=lookback_days)).date()
    last_week_start = (now - timedelta(days=lookback_days * 2)).date()

    query_current = f"""
        SELECT liquidation_probability, risk_tier
        FROM predictions_log
        WHERE scored_at::date >= '{this_week_start}'
    """
    query_reference = f"""
        SELECT liquidation_probability, risk_tier
        FROM predictions_log
        WHERE scored_at::date >= '{last_week_start}'
          AND scored_at::date < '{this_week_start}'
    """

    current_preds = pd.read_sql(query_current, conn)
    ref_preds = pd.read_sql(query_reference, conn)
    conn.close()

    return ref_preds, current_preds


def run_data_drift_report(
    reference: pd.DataFrame,
    current: pd.DataFrame,
    report_date: str,
) -> tuple[bool, float]:
    """
    Run Evidently data drift report.

    Returns (drift_detected, drift_fraction).
    """
    REPORTS_DIR.mkdir(exist_ok=True)

    column_mapping = ColumnMapping(
        target=LABEL_COL,
        numerical_features=[f for f in ALL_FEATURES if f != "protocol_encoded"],
        categorical_features=["protocol_encoded", "is_eth_dominant_collateral"],
    )

    report = Report(
        metrics=[
            DatasetDriftMetric(),
            DataDriftPreset(),
            *[ColumnDriftMetric(column_name=f) for f in ALL_FEATURES[:10]],
        ]
    )
    report.run(reference_data=reference, current_data=current, column_mapping=column_mapping)

    output_path = REPORTS_DIR / f"drift_{report_date}.html"
    report.save_html(str(output_path))
    logger.info("Drift report saved: %s", output_path)

    report_dict = report.as_dict()
    dataset_drift = report_dict["metrics"][0]["result"]
    drift_fraction = dataset_drift.get("share_of_drifted_columns", 0.0)
    drift_detected = drift_fraction >= DRIFT_THRESHOLD

    logger.info(
        "Data drift: %.1f%% of features drifted (threshold: %.0f%%) — drift_detected=%s",
        drift_fraction * 100,
        DRIFT_THRESHOLD * 100,
        drift_detected,
    )

    return drift_detected, drift_fraction


def run_prediction_drift_report(
    ref_preds: pd.DataFrame,
    current_preds: pd.DataFrame,
    report_date: str,
) -> dict:
    """Summarize prediction distribution shift between two time windows."""
    if ref_preds.empty or current_preds.empty:
        logger.warning("Not enough prediction data for drift comparison.")
        return {}

    stats = {
        "ref_mean_probability": ref_preds["liquidation_probability"].mean(),
        "current_mean_probability": current_preds["liquidation_probability"].mean(),
        "probability_shift": (
            current_preds["liquidation_probability"].mean()
            - ref_preds["liquidation_probability"].mean()
        ),
        "current_pct_at_risk_or_higher": (
            (current_preds["risk_tier"].isin(["CRITICAL", "AT_RISK"])).mean()
        ),
    }

    prob_shift = abs(stats["probability_shift"])
    if prob_shift > 0.10:
        logger.warning(
            "Prediction drift detected: mean probability shifted by %.3f — investigate data quality.",
            stats["probability_shift"],
        )

    return stats


def run(lookback_days: int = 7) -> bool:
    """
    Run full drift monitoring pipeline.

    Returns True if retraining should be triggered.
    """
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)

    report_date = datetime.now(tz=UTC).strftime("%Y-%m-%d")
    trigger_retrain = False

    reference = load_reference_data()
    current = load_current_data(lookback_days=lookback_days)

    if len(current) == 0:
        logger.warning("No current data for drift comparison.")
        return False

    with mlflow.start_run(run_name=f"monitoring-{report_date}", tags={"type": "monitoring"}):
        # Data drift
        drift_detected, drift_fraction = run_data_drift_report(reference, current, report_date)
        mlflow.log_metric("drift_fraction", drift_fraction)
        mlflow.log_metric("drift_detected", int(drift_detected))
        mlflow.log_artifact(str(REPORTS_DIR / f"drift_{report_date}.html"))

        # Prediction drift
        try:
            ref_preds, current_preds = load_prediction_data(lookback_days=lookback_days)
            pred_stats = run_prediction_drift_report(ref_preds, current_preds, report_date)
            for k, v in pred_stats.items():
                mlflow.log_metric(k, v)
        except Exception:
            logger.warning("Prediction drift check failed (predictions_log may be empty).")

        trigger_retrain = drift_detected

    if trigger_retrain:
        logger.warning("DRIFT DETECTED: triggering retraining.")
    else:
        logger.info("No significant drift detected.")

    return trigger_retrain


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Evidently drift monitoring")
    parser.add_argument("--lookback-days", type=int, default=7)
    args = parser.parse_args()

    should_retrain = run(lookback_days=args.lookback_days)
    sys.exit(1 if should_retrain else 0)


if __name__ == "__main__":
    main()
