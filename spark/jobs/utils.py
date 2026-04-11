"""
Shared SparkSession factory for bronze and silver jobs.

The session is configured to use:
  - Nessie as the Iceberg catalog (name: ``nessie``)
  - MinIO as the S3-compatible object store via Iceberg S3FileIO
  - Hadoop S3A connector for reading raw JSON from MinIO

All credentials are read from environment variables so the same code runs
in Docker Compose and on a real cluster without changes.
"""

from __future__ import annotations

import os

from pyspark.sql import SparkSession


def get_spark_session(app_name: str) -> SparkSession:
    """
    Build and return a SparkSession wired to Nessie + MinIO.

    The JARs (Iceberg, Nessie extensions, AWS bundle) are listed in
    ``spark.jars.packages`` and are resolved at session startup via Ivy.
    If they are already present in the Ivy cache (pre-downloaded in the
    Docker image) the download is skipped.
    """
    nessie_uri = os.getenv("NESSIE_URI", "http://nessie:19120/api/v2")
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    minio_access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    minio_secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")

    return (
        SparkSession.builder.appName(app_name)
        # ── JAR resolution ─────────────────────────────────────────────────
        .config(
            "spark.jars.packages",
            ",".join([
                "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1",
                "org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.99.0",
                "org.apache.iceberg:iceberg-aws-bundle:1.6.1",
            ]),
        )
        .config("spark.jars.ivy", "/tmp/.ivy2")
        # ── SQL extensions ─────────────────────────────────────────────────
        .config(
            "spark.sql.extensions",
            ",".join([
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
                "org.projectnessie.spark.extensions.NessieSparkSessionExtensions",
            ]),
        )
        # ── Nessie catalog ─────────────────────────────────────────────────
        .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog")
        .config(
            "spark.sql.catalog.nessie.catalog-impl",
            "org.apache.iceberg.nessie.NessieCatalog",
        )
        .config("spark.sql.catalog.nessie.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.nessie.uri", nessie_uri)
        .config("spark.sql.catalog.nessie.ref", "main")
        .config("spark.sql.catalog.nessie.cache-enabled", "false")
        .config("spark.sql.catalog.nessie.warehouse", "s3://lakehouse/warehouse")
        # Iceberg S3FileIO credentials (used by catalog metadata + data writes)
        .config("spark.sql.catalog.nessie.s3.endpoint", minio_endpoint)
        .config("spark.sql.catalog.nessie.s3.path-style-access", "true")
        .config("spark.sql.catalog.nessie.s3.access-key-id", minio_access_key)
        .config("spark.sql.catalog.nessie.s3.secret-access-key", minio_secret_key)
        .config("spark.sql.catalog.nessie.s3.region", "us-east-1")
        # ── Hadoop S3A (raw JSON reads) ────────────────────────────────────
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        # ── Misc ───────────────────────────────────────────────────────────
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()
    )
