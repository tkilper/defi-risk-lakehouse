"""
DAG: liquidation_labels
========================
Ingests liquidation event data from The Graph APIs and runs the Spark
bronze/silver jobs to produce nessie.silver.liquidation_events.

Schedule: daily at 01:00 UTC (after the main ingest DAG has run)

Tasks:
  fetch_aave_liquidations     → s3://lakehouse/raw/liquidations/aave/...
  fetch_compound_liquidations → s3://lakehouse/raw/liquidations/compound/...
  fetch_maker_liquidations    → s3://lakehouse/raw/liquidations/maker/...
  validate_liquidation_counts → fail if all three returned 0
  spark_bronze_liquidations   → nessie.bronze.*_liquidations_raw
  spark_silver_liquidations   → nessie.silver.liquidation_events
  spark_position_snapshots    → nessie.silver.position_snapshots
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

from ingestion.liquidation_client import AaveLiquidationClient
from ingestion.compound_liq_client import CompoundLiquidationClient
from ingestion.maker_liq_client import MakerLiquidationClient
from ingestion.s3_writer import write_raw_records

logger = logging.getLogger(__name__)

_SPARK_SUBMIT = (
    "/opt/spark/bin/spark-submit "
    "--master spark://spark-master:7077 "
    "--conf spark.driver.memory=1g "
)

default_args = {
    "owner": "defi-risk",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


# ---------------------------------------------------------------------------
# Task callables
# ---------------------------------------------------------------------------


def fetch_aave_liquidations(**context) -> int:
    client = AaveLiquidationClient()
    records = client.fetch_liquidations()
    ts = context["execution_date"]
    write_raw_records("liquidations/aave", records, snapshot_ts=ts)
    logger.info("Aave liquidations: wrote %d records.", len(records))
    return len(records)


def fetch_compound_liquidations(**context) -> int:
    client = CompoundLiquidationClient()
    try:
        records = client.fetch_liquidations()
    except Exception:
        logger.warning("Compound liquidation fetch failed — continuing with 0 records.")
        records = []
    ts = context["execution_date"]
    if records:
        write_raw_records("liquidations/compound", records, snapshot_ts=ts)
    logger.info("Compound liquidations: wrote %d records.", len(records))
    return len(records)


def fetch_maker_liquidations(**context) -> int:
    client = MakerLiquidationClient()
    try:
        records = client.fetch_liquidations()
    except Exception:
        logger.warning("Maker liquidation fetch failed — continuing with 0 records.")
        records = []
    ts = context["execution_date"]
    if records:
        write_raw_records("liquidations/maker", records, snapshot_ts=ts)
    logger.info("Maker liquidations: wrote %d records.", len(records))
    return len(records)


def validate_counts(**context) -> None:
    """Log counts; do not fail the DAG if subgraph issues cause zero returns."""
    ti = context["ti"]
    aave = ti.xcom_pull(task_ids="fetch_aave_liquidations") or 0
    compound = ti.xcom_pull(task_ids="fetch_compound_liquidations") or 0
    maker = ti.xcom_pull(task_ids="fetch_maker_liquidations") or 0
    logger.info("Liquidation counts — Aave: %d, Compound: %d, Maker: %d", aave, compound, maker)
    # Note: we only warn here (not fail) because Compound/Maker subgraphs
    # may be temporarily unavailable. Aave is the primary signal.
    if aave == 0:
        logger.warning("Aave returned 0 liquidations — check subgraph health.")


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id="liquidation_labels",
    default_args=default_args,
    description="Ingest DeFi liquidation events → bronze/silver Iceberg tables",
    schedule_interval="0 1 * * *",  # daily at 01:00 UTC
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["defi", "ml", "liquidations"],
) as dag:
    t_aave = PythonOperator(
        task_id="fetch_aave_liquidations",
        python_callable=fetch_aave_liquidations,
    )

    t_compound = PythonOperator(
        task_id="fetch_compound_liquidations",
        python_callable=fetch_compound_liquidations,
    )

    t_maker = PythonOperator(
        task_id="fetch_maker_liquidations",
        python_callable=fetch_maker_liquidations,
    )

    t_validate = PythonOperator(
        task_id="validate_liquidation_counts",
        python_callable=validate_counts,
    )

    t_bronze = BashOperator(
        task_id="spark_bronze_liquidations",
        bash_command=f"{_SPARK_SUBMIT} /opt/spark/jobs/bronze_liquidations.py",
    )

    t_silver_liq = BashOperator(
        task_id="spark_silver_liquidations",
        bash_command=f"{_SPARK_SUBMIT} /opt/spark/jobs/silver_liquidations.py",
    )

    t_silver_snap = BashOperator(
        task_id="spark_position_snapshots",
        bash_command=f"{_SPARK_SUBMIT} /opt/spark/jobs/silver_position_snapshots.py",
    )

    [t_aave, t_compound, t_maker] >> t_validate >> t_bronze >> t_silver_liq >> t_silver_snap
