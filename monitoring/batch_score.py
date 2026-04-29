"""
Batch scoring — scores all currently open positions and writes predictions
to the predictions_log table in Postgres.

This runs as a task in the batch_score_dag Airflow DAG. It reads open
positions from nessie.gold.ml_features (those without a label, i.e. the
last 24h), calls the FastAPI endpoint, and logs results to Postgres.

The predictions_log table is what Evidently reads for prediction drift monitoring.

Usage:
    python monitoring/batch_score.py
"""

from __future__ import annotations

import logging
import os
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
import psycopg2
import requests
import trino

sys.path.insert(0, str(Path(__file__).parents[1]))
from features.feature_definitions import ALL_FEATURES

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

API_URL = os.getenv("API_URL", "http://localhost:8000")
TRINO_HOST = os.getenv("TRINO_HOST", "localhost")
TRINO_PORT = int(os.getenv("TRINO_PORT", "8081"))

_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS predictions_log (
    id                      SERIAL PRIMARY KEY,
    snapshot_id             TEXT,
    protocol                TEXT,
    user_address            TEXT,
    snapshot_time           TIMESTAMP,
    liquidation_probability DOUBLE PRECISION,
    risk_tier               TEXT,
    model_version           TEXT,
    scored_at               TIMESTAMP DEFAULT NOW()
);
"""


def _pg_conn():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        dbname=os.getenv("POSTGRES_DB", "airflow"),
        user=os.getenv("POSTGRES_USER", "airflow"),
        password=os.getenv("POSTGRES_PASSWORD", "airflow"),
    )


def ensure_predictions_table() -> None:
    conn = _pg_conn()
    with conn.cursor() as cur:
        cur.execute(_CREATE_TABLE_SQL)
    conn.commit()
    conn.close()


def load_open_positions() -> pd.DataFrame:
    """
    Load positions from the last 24h that do not yet have a confirmed
    liquidation label — these are the 'open' positions to score.
    """
    cutoff = (datetime.now(tz=UTC) - timedelta(hours=24)).isoformat()
    conn = trino.dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user="scoring",
        catalog="lakehouse",
        schema="gold",
    )
    query = f"""
        SELECT
            snapshot_id,
            user_address,
            protocol,
            snapshot_time,
            {', '.join(ALL_FEATURES)}
        FROM ml_features
        WHERE snapshot_time > TIMESTAMP '{cutoff}'
    """
    df = pd.read_sql(query, conn)
    logger.info("Loaded %d open positions for scoring.", len(df))
    return df


def score_position(row: dict[str, Any]) -> dict[str, Any] | None:
    """Call the FastAPI /predict endpoint for a single position."""
    payload = {col: row.get(col) for col in ALL_FEATURES}
    # Replace None with None — FastAPI/pydantic handles optional fields
    try:
        resp = requests.post(
            f"{API_URL}/predict",
            json=payload,
            timeout=5,
        )
        resp.raise_for_status()
        return resp.json()
    except Exception:
        logger.warning("Failed to score position %s", row.get("snapshot_id"))
        return None


def write_predictions(predictions: list[dict[str, Any]]) -> int:
    """Bulk-insert predictions into the Postgres predictions_log table."""
    if not predictions:
        return 0

    conn = _pg_conn()
    rows_written = 0
    with conn.cursor() as cur:
        for pred in predictions:
            if pred is None:
                continue
            cur.execute(
                """
                INSERT INTO predictions_log
                    (snapshot_id, protocol, user_address, snapshot_time,
                     liquidation_probability, risk_tier, model_version, scored_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
                """,
                (
                    pred.get("snapshot_id"),
                    pred.get("protocol"),
                    pred.get("user_address"),
                    pred.get("snapshot_time"),
                    pred.get("liquidation_probability"),
                    pred.get("risk_tier"),
                    pred.get("model_version"),
                ),
            )
            rows_written += 1
    conn.commit()
    conn.close()
    logger.info("Wrote %d predictions to predictions_log.", rows_written)
    return rows_written


def run() -> int:
    ensure_predictions_table()
    positions = load_open_positions()

    if len(positions) == 0:
        logger.info("No open positions to score.")
        return 0

    results = []
    for _, row_series in positions.iterrows():
        row = row_series.to_dict()
        result = score_position(row)
        if result:
            result["snapshot_id"] = row.get("snapshot_id")
            result["protocol"] = row.get("protocol")
            result["user_address"] = row.get("user_address")
            result["snapshot_time"] = row.get("snapshot_time")
            results.append(result)

    return write_predictions(results)


if __name__ == "__main__":
    written = run()
    logger.info("Batch scoring complete: %d predictions written.", written)
