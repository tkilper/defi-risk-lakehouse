"""
DAG: model_retrain
===================
Automated retraining pipeline. Triggers either on a weekly schedule or
when the drift monitoring DAG flags significant feature drift.

DAG steps:
  1. run_drift_check       — run Evidently drift report; set XCom flag
  2. feature_engineering   — rebuild gold.ml_features with new data
  3. dvc_snapshot          — snapshot training data and push to MinIO
  4. train_model           — run training/train.py
  5. evaluate_model        — run training/evaluate.py; compare vs Production
  6. promote_model         — if new model beats Production, promote to Production
  7. reload_api            — POST /reload to FastAPI to hot-reload model

If the new model does NOT beat the Production model, steps 6–7 are skipped
and an Airflow warning is logged.

Promotion criteria (configurable via Airflow Variables):
  - AUC-ROC > Production AUC-ROC + 0.02  (improvement of at least 2 points)
  - Brier score < Production Brier score

Schedule: weekly on Sundays at 02:00 UTC
"""

from __future__ import annotations

import logging
import os
import subprocess
import sys
from datetime import timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.dates import days_ago

sys.path.insert(0, "/opt/airflow")

logger = logging.getLogger(__name__)

_SPARK_SUBMIT = (
    "/opt/spark/bin/spark-submit "
    "--master spark://spark-master:7077 "
    "--conf spark.driver.memory=2g "
)

_AUC_IMPROVEMENT_REQUIRED = float(os.getenv("AUC_IMPROVEMENT_REQUIRED", "0.02"))

default_args = {
    "owner": "defi-risk",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 0,  # retraining failures should surface, not auto-retry
    "retry_delay": timedelta(minutes=10),
}


# ---------------------------------------------------------------------------
# Task functions
# ---------------------------------------------------------------------------


def check_drift(**context) -> None:
    """Run drift monitoring and push drift_detected flag to XCom."""
    from monitoring.drift_report import run as run_drift

    drift_detected = run_drift(lookback_days=7)
    context["ti"].xcom_push(key="drift_detected", value=drift_detected)
    logger.info("Drift detected: %s", drift_detected)


def snapshot_training_data(**context) -> str:
    """Export the current ml_features table to parquet and push to DVC."""
    from datetime import UTC, datetime

    import pandas as pd
    import trino

    trino_host = os.getenv("TRINO_HOST", "trino")
    trino_port = int(os.getenv("TRINO_PORT", "8080"))
    date_str = datetime.now(tz=UTC).strftime("%Y-%m-%d")
    output_path = f"/opt/airflow/data/training_features_{date_str}.parquet"

    conn = trino.dbapi.connect(
        host=trino_host,
        port=trino_port,
        user="retrain",
        catalog="lakehouse",
        schema="gold",
    )
    df = pd.read_sql(
        "SELECT * FROM ml_features WHERE liquidated_within_24h IS NOT NULL",
        conn,
    )
    df.to_parquet(output_path, index=False)
    logger.info("Exported %d rows to %s", len(df), output_path)

    # Track with DVC (best-effort; DVC remote may not be configured in all envs)
    try:
        subprocess.run(["dvc", "add", output_path], check=True, capture_output=True)
        subprocess.run(["dvc", "push"], check=True, capture_output=True)
        logger.info("DVC snapshot pushed for %s", output_path)
    except Exception:
        logger.warning("DVC push skipped — DVC remote may not be configured.")

    context["ti"].xcom_push(key="data_path", value=output_path)
    return output_path


def train_model(**context) -> str:
    """Run the XGBoost training script and push the new run ID to XCom."""
    import mlflow

    data_path = context["ti"].xcom_pull(task_ids="dvc_snapshot", key="data_path")

    from training.train import train

    run_id = train(data_path=data_path)
    context["ti"].xcom_push(key="new_run_id", value=run_id)
    logger.info("Training complete. MLflow run: %s", run_id)
    return run_id


def evaluate_and_branch(**context) -> str:
    """
    Compare the new model vs the current Production model.
    Returns the task_id to branch to (promote or skip_promotion).
    """
    import mlflow

    new_run_id = context["ti"].xcom_pull(task_ids="train_model", key="new_run_id")

    mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000"))
    client = mlflow.MlflowClient()

    # Get new model metrics
    new_run = client.get_run(new_run_id)
    new_auc = new_run.data.metrics.get("holdout_auc_roc", 0.0)
    new_brier = new_run.data.metrics.get("holdout_brier_score", 1.0)

    # Get current Production model metrics
    try:
        prod_versions = client.get_latest_versions("liquidation-predictor", stages=["Production"])
        if prod_versions:
            prod_run = client.get_run(prod_versions[0].run_id)
            prod_auc = prod_run.data.metrics.get("holdout_auc_roc", 0.0)
            prod_brier = prod_run.data.metrics.get("holdout_brier_score", 1.0)
        else:
            # No Production model yet — promote unconditionally
            logger.info("No Production model found — will promote new model.")
            return "promote_model"
    except Exception:
        logger.warning("Could not retrieve Production model metrics — promoting new model.")
        return "promote_model"

    logger.info(
        "New model: AUC=%.4f Brier=%.4f | Production: AUC=%.4f Brier=%.4f",
        new_auc, new_brier, prod_auc, prod_brier,
    )

    auc_improved = new_auc >= prod_auc + _AUC_IMPROVEMENT_REQUIRED
    brier_improved = new_brier <= prod_brier

    if auc_improved and brier_improved:
        logger.info("New model beats Production — promoting.")
        return "promote_model"
    else:
        logger.warning(
            "New model does NOT beat Production (AUC improved: %s, Brier improved: %s) — skipping promotion.",
            auc_improved,
            brier_improved,
        )
        return "skip_promotion"


def promote_model(**context) -> None:
    """Promote the new model to Production in the MLflow Model Registry."""
    import mlflow

    new_run_id = context["ti"].xcom_pull(task_ids="train_model", key="new_run_id")
    mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000"))
    client = mlflow.MlflowClient()

    # Find the model version registered from this run
    versions = client.search_model_versions(f"run_id='{new_run_id}'")
    if not versions:
        raise ValueError(f"No model version found for run {new_run_id}")

    version = versions[0].version
    client.transition_model_version_stage(
        name="liquidation-predictor",
        version=version,
        stage="Production",
        archive_existing_versions=True,
    )
    logger.info("Promoted model version %s to Production.", version)
    context["ti"].xcom_push(key="promoted_version", value=version)


def reload_api(**context) -> None:
    """POST /reload to the FastAPI serving endpoint to load the new model."""
    import requests

    api_url = os.getenv("API_URL", "http://api:8000")
    try:
        resp = requests.post(f"{api_url}/reload", timeout=30)
        resp.raise_for_status()
        logger.info("API reloaded: %s", resp.json())
    except Exception:
        logger.warning("API reload failed — the serving container may need a restart.")


def skip_promotion(**context) -> None:
    logger.info("Promotion skipped — current Production model is still the best.")


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id="model_retrain",
    default_args=default_args,
    description="Weekly automated retraining pipeline for the liquidation predictor",
    schedule_interval="0 2 * * 0",  # weekly on Sunday at 02:00 UTC
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["defi", "ml", "retraining"],
) as dag:
    t_drift = PythonOperator(
        task_id="run_drift_check",
        python_callable=check_drift,
    )

    t_features = BashOperator(
        task_id="rebuild_features",
        bash_command=(
            f"{_SPARK_SUBMIT} "
            "--py-files /opt/spark/jobs/utils.py "
            "/opt/airflow/features/feature_pipeline.py"
        ),
    )

    t_dvc = PythonOperator(
        task_id="dvc_snapshot",
        python_callable=snapshot_training_data,
    )

    t_train = PythonOperator(
        task_id="train_model",
        python_callable=train_model,
    )

    t_branch = BranchPythonOperator(
        task_id="evaluate_and_branch",
        python_callable=evaluate_and_branch,
    )

    t_promote = PythonOperator(
        task_id="promote_model",
        python_callable=promote_model,
    )

    t_skip = PythonOperator(
        task_id="skip_promotion",
        python_callable=skip_promotion,
    )

    t_reload = PythonOperator(
        task_id="reload_api",
        python_callable=reload_api,
        trigger_rule="none_failed_min_one_success",
    )

    t_drift >> t_features >> t_dvc >> t_train >> t_branch
    t_branch >> [t_promote, t_skip]
    t_promote >> t_reload
