"""
DAG: defi_ingest
================
Ingests raw DeFi position data from The Graph subgraph APIs into MinIO.

Schedule: every 6 hours  (positions change with each Ethereum block)

Tasks (run in parallel):
  fetch_aave_positions     → s3://lakehouse/raw/aave/date=.../hour=.../data.json
  fetch_compound_positions → s3://lakehouse/raw/compound/...
  fetch_maker_vaults       → s3://lakehouse/raw/maker/...
  validate_raw_counts      → fails the DAG if all three protocols returned 0 records

On success, the ``defi_transform`` DAG is triggered via a TriggerDagRunOperator.
"""

from __future__ import annotations

import logging
import sys
from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago

# Ensure the project root is on sys.path so imports work inside the container
sys.path.insert(0, "/opt/airflow")

from ingestion.aave_client import AaveClient
from ingestion.compound_client import CompoundClient
from ingestion.maker_client import MakerClient
from ingestion.s3_writer import write_raw_records

logger = logging.getLogger(__name__)

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


def fetch_aave(**context) -> int:
    client = AaveClient()
    records = client.fetch_borrow_positions()
    ts = context["execution_date"]
    write_raw_records("aave", records, snapshot_ts=ts)
    logger.info("Aave: wrote %d records.", len(records))
    return len(records)


def fetch_compound(**context) -> int:
    client = CompoundClient()
    records = client.fetch_borrow_positions()
    ts = context["execution_date"]
    write_raw_records("compound", records, snapshot_ts=ts)
    logger.info("Compound: wrote %d records.", len(records))
    return len(records)


def fetch_maker(**context) -> int:
    client = MakerClient()
    vaults = client.fetch_active_vaults()
    ts = context["execution_date"]
    write_raw_records("maker", vaults, snapshot_ts=ts)
    logger.info("Maker: wrote %d vaults.", len(vaults))
    return len(vaults)


def validate_raw_counts(**context) -> None:
    """Fail if ALL three protocols returned 0 records (likely an API outage)."""
    ti = context["ti"]
    aave_count = ti.xcom_pull(task_ids="fetch_aave_positions") or 0
    compound_count = ti.xcom_pull(task_ids="fetch_compound_positions") or 0
    maker_count = ti.xcom_pull(task_ids="fetch_maker_vaults") or 0
    total = aave_count + compound_count + maker_count
    logger.info(
        "Raw record counts — Aave: %d, Compound: %d, Maker: %d",
        aave_count,
        compound_count,
        maker_count,
    )
    if total == 0:
        raise ValueError(
            "All three protocol clients returned 0 records. "
            "Possible API outage or misconfiguration."
        )


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id="defi_ingest",
    default_args=default_args,
    description="Ingest DeFi borrow positions from The Graph → MinIO raw zone",
    schedule_interval="0 */6 * * *",  # every 6 hours
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["defi", "ingestion", "thegraph"],
) as dag:
    t_aave = PythonOperator(
        task_id="fetch_aave_positions",
        python_callable=fetch_aave,
    )

    t_compound = PythonOperator(
        task_id="fetch_compound_positions",
        python_callable=fetch_compound,
    )

    t_maker = PythonOperator(
        task_id="fetch_maker_vaults",
        python_callable=fetch_maker,
    )

    t_validate = PythonOperator(
        task_id="validate_raw_counts",
        python_callable=validate_raw_counts,
    )

    t_trigger_transform = TriggerDagRunOperator(
        task_id="trigger_defi_transform",
        trigger_dag_id="defi_transform",
        wait_for_completion=False,
        reset_dag_run=True,
    )

    # Parallel ingestion → validation → trigger downstream transform DAG
    [t_aave, t_compound, t_maker] >> t_validate >> t_trigger_transform
