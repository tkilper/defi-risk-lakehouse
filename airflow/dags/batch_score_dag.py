"""
DAG: batch_score
=================
Scores all currently open DeFi borrow positions and writes predictions to
the predictions_log table in Postgres.

Runs daily after the feature_engineering_dag has completed.

Schedule: daily at 05:00 UTC
"""

from __future__ import annotations

import logging
import sys
from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

sys.path.insert(0, "/opt/airflow")

logger = logging.getLogger(__name__)

default_args = {
    "owner": "defi-risk",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def run_batch_scoring(**context) -> int:
    from monitoring.batch_score import run

    written = run()
    logger.info("Batch scoring wrote %d predictions.", written)
    return written


with DAG(
    dag_id="batch_score",
    default_args=default_args,
    description="Score open DeFi positions and log predictions to Postgres",
    schedule_interval="0 5 * * *",  # daily at 05:00 UTC
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["defi", "ml", "scoring"],
) as dag:
    PythonOperator(
        task_id="score_open_positions",
        python_callable=run_batch_scoring,
    )
