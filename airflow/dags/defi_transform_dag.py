"""
DAG: defi_transform
====================
Transforms raw DeFi data through the lakehouse layers:

  1. spark_bronze  — raw JSON (MinIO) → Iceberg bronze tables (Nessie)
  2. spark_silver  — bronze Iceberg → silver Iceberg (normalised USD values)
  3. dbt_run       — silver Iceberg → gold Iceberg (health factors, cascade scenarios)
  4. dbt_test      — run all dbt schema + custom tests against gold tables
  5. notify_done   — log completion summary

This DAG is normally triggered by ``defi_ingest`` but can also be run manually.
"""

from __future__ import annotations

import logging
import sys
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

sys.path.insert(0, "/opt/airflow")

logger = logging.getLogger(__name__)

default_args = {
    "owner": "defi-risk",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

DBT_DIR = "/opt/airflow/dbt/defi_risk"
DBT_BIN = "/opt/airflow/dbt-venv/bin/dbt"
SPARK_JOBS_DIR = "/opt/airflow/spark/jobs"


def log_completion(**context) -> None:
    logger.info(
        "DeFi Risk transform pipeline complete for run: %s",
        context.get("run_id"),
    )


with DAG(
    dag_id="defi_transform",
    default_args=default_args,
    description="Spark bronze/silver ETL + dbt gold models for DeFi risk metrics",
    schedule_interval=None,  # Triggered by defi_ingest; no independent schedule
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["defi", "spark", "dbt", "iceberg"],
) as dag:
    # ── 1. Bronze: raw JSON → Iceberg bronze tables ──────────────────────────
    t_spark_bronze = BashOperator(
        task_id="spark_bronze",
        bash_command=(f"cd {SPARK_JOBS_DIR} && PYTHONPATH=/opt/airflow python bronze_loader.py"),
        execution_timeout=timedelta(minutes=30),
    )

    # ── 2. Silver: normalise + compute USD values ─────────────────────────────
    t_spark_silver = BashOperator(
        task_id="spark_silver",
        bash_command=(
            f"cd {SPARK_JOBS_DIR} && PYTHONPATH=/opt/airflow python silver_transformer.py"
        ),
        execution_timeout=timedelta(minutes=30),
    )

    # ── 3. dbt run: silver → gold (health factors, cascade scenarios) ─────────
    t_dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"{DBT_BIN} deps "
            f"--project-dir {DBT_DIR} "
            f"--profiles-dir {DBT_DIR} "
            f"&& "
            f"{DBT_BIN} run "
            f"--project-dir {DBT_DIR} "
            f"--profiles-dir {DBT_DIR} "
            f"--target prod "
            f"--no-use-colors"
        ),
        execution_timeout=timedelta(minutes=20),
    )

    # ── 4. dbt test: schema tests + custom SQL tests ──────────────────────────
    t_dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"{DBT_BIN} test "
            f"--project-dir {DBT_DIR} "
            f"--profiles-dir {DBT_DIR} "
            f"--target prod "
            f"--no-use-colors"
        ),
        execution_timeout=timedelta(minutes=15),
    )

    # ── 5. Notify completion ──────────────────────────────────────────────────
    t_done = PythonOperator(
        task_id="notify_done",
        python_callable=log_completion,
    )

    # Enforce linear dependency: bronze → silver → dbt run → dbt test → done
    t_spark_bronze >> t_spark_silver >> t_dbt_run >> t_dbt_test >> t_done
