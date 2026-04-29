"""
DAG: feature_engineering
=========================
Builds the ML feature table (nessie.gold.ml_features) by running the
Spark feature pipeline after the liquidation labels are available.

Schedule: daily at 03:00 UTC (after liquidation_labels_dag at 01:00)

Tasks:
  spark_feature_pipeline → nessie.gold.ml_features
  validate_feature_table → assert row count > 0 and label rate is sane
"""

from __future__ import annotations

import logging
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

logger = logging.getLogger(__name__)

_SPARK_SUBMIT = (
    "/opt/spark/bin/spark-submit "
    "--master spark://spark-master:7077 "
    "--conf spark.driver.memory=2g "
)

default_args = {
    "owner": "defi-risk",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}


def validate_feature_table(**context) -> None:
    """
    Query Trino to validate that the feature table was written with reasonable stats.
    Fails the task if the table appears empty or the positive rate is > 50%
    (which would suggest a label leakage bug).
    """
    import os

    import trino

    host = os.getenv("TRINO_HOST", "trino")
    port = int(os.getenv("TRINO_PORT", "8080"))

    conn = trino.dbapi.connect(
        host=host,
        port=port,
        user="airflow",
        catalog="lakehouse",
        schema="gold",
    )
    cursor = conn.cursor()
    cursor.execute(
        "SELECT COUNT(*) AS total, SUM(liquidated_within_24h) AS positives "
        "FROM ml_features"
    )
    row = cursor.fetchone()
    total, positives = row[0], row[1] or 0
    positive_rate = positives / max(total, 1)

    logger.info(
        "ml_features: %d rows, %d positives (%.2f%% positive rate)",
        total,
        positives,
        100.0 * positive_rate,
    )

    if total == 0:
        raise ValueError("ml_features table is empty — feature pipeline may have failed.")

    if positive_rate > 0.50:
        raise ValueError(
            f"Positive rate {positive_rate:.2%} > 50% — possible label leakage. "
            "Check the 24h label join logic."
        )


with DAG(
    dag_id="feature_engineering",
    default_args=default_args,
    description="Build nessie.gold.ml_features for liquidation predictor training",
    schedule_interval="0 3 * * *",  # daily at 03:00 UTC
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["defi", "ml", "features"],
) as dag:
    t_features = BashOperator(
        task_id="spark_feature_pipeline",
        bash_command=(
            f"{_SPARK_SUBMIT} "
            "--py-files /opt/spark/jobs/utils.py "
            "/opt/airflow/features/feature_pipeline.py"
        ),
    )

    t_validate = PythonOperator(
        task_id="validate_feature_table",
        python_callable=validate_feature_table,
    )

    t_features >> t_validate
