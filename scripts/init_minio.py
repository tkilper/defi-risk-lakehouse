"""
MinIO initialisation script.

Creates the required buckets and folder prefixes in MinIO.
Idempotent — safe to run multiple times.

Run via: make init-buckets
Or in Docker: docker compose exec airflow-scheduler python /opt/airflow/scripts/init_minio.py
"""

from __future__ import annotations

import os
import sys

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

BUCKET = os.getenv("MINIO_BUCKET", "lakehouse")
PREFIXES = [
    "raw/aave/",
    "raw/compound/",
    "raw/maker/",
    "warehouse/",
]


def get_client():
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def main() -> None:
    client = get_client()

    # Create bucket
    try:
        client.create_bucket(Bucket=BUCKET)
        print(f"Created bucket: {BUCKET}")
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code in ("BucketAlreadyExists", "BucketAlreadyOwnedByYou"):
            print(f"Bucket already exists: {BUCKET}")
        else:
            print(f"Error creating bucket: {e}", file=sys.stderr)
            sys.exit(1)

    # Create placeholder objects for the folder prefixes
    for prefix in PREFIXES:
        key = f"{prefix}.keep"
        try:
            client.put_object(Bucket=BUCKET, Key=key, Body=b"")
            print(f"Ensured prefix: s3://{BUCKET}/{prefix}")
        except ClientError as e:
            print(f"Warning: could not create prefix {prefix}: {e}", file=sys.stderr)

    print("MinIO initialisation complete.")


if __name__ == "__main__":
    main()
