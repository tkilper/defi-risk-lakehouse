"""
Utility for writing raw protocol data to MinIO (S3-compatible).

Data is written as newline-delimited JSON (NDJSON) partitioned by:
  s3://lakehouse/raw/{protocol}/date={YYYY-MM-DD}/hour={HH}/data.json

This partitioning matches the read pattern in the Spark bronze loader.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any

import boto3
from botocore.config import Config

logger = logging.getLogger(__name__)

BUCKET = os.getenv("MINIO_BUCKET", "lakehouse")
_RAW_PREFIX = "raw"


def _get_s3_client() -> Any:
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def write_raw_records(
    protocol: str,
    records: list[dict[str, Any]],
    snapshot_ts: datetime | None = None,
) -> str:
    """
    Serialize ``records`` as NDJSON and upload to MinIO.

    Parameters
    ----------
    protocol:
        One of ``"aave"``, ``"compound"``, ``"maker"``.
    records:
        Raw position / vault dicts from the GraphQL client.
    snapshot_ts:
        Timestamp for the partition key.  Defaults to now (UTC).

    Returns
    -------
    str
        The S3 key the data was written to.
    """
    ts = snapshot_ts or datetime.now(tz=timezone.utc)
    date_str = ts.strftime("%Y-%m-%d")
    hour_str = ts.strftime("%H")

    s3_key = f"{_RAW_PREFIX}/{protocol}/date={date_str}/hour={hour_str}/data.json"

    ndjson_body = "\n".join(json.dumps(r) for r in records)

    client = _get_s3_client()
    client.put_object(
        Bucket=BUCKET,
        Key=s3_key,
        Body=ndjson_body.encode("utf-8"),
        ContentType="application/x-ndjson",
    )
    logger.info("Wrote %d records to s3://%s/%s", len(records), BUCKET, s3_key)
    return s3_key
