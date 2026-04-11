"""
Integration test fixtures.

These tests require all Docker Compose services to be running.
Run with: pytest tests/integration/ -m integration
Or via:   make test-integration
"""

import json
import os

import boto3
import pytest
from botocore.config import Config


@pytest.fixture(scope="session")
def minio_client():
    """Boto3 S3 client pointing at the MinIO container."""
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


@pytest.fixture
def sample_aave_records():
    """Minimal Aave-like raw records for seeding integration tests."""
    return [
        {
            "id": f"0xuser{i}-0xweth",
            "user": {"id": f"0xuser{i}"},
            "reserve": {
                "id": "0xweth",
                "symbol": "WETH",
                "decimals": 18,
                "underlyingAsset": "0xweth",
                "usageAsCollateralEnabled": True,
                "reserveLiquidationThreshold": "8000",
                "baseLTVasCollateral": "7500",
                "reserveLiquidationBonus": "10500",
                "price": {"priceInEth": "1000000000000000000"},
            },
            "currentATokenBalance": str(int(2e18)),
            "currentVariableDebt": str(int(1e18)),
            "currentStableDebt": "0",
            "usageAsCollateralEnabledOnUser": True,
            "liquidityRate": "0.03",
            "variableBorrowRate": "0.05",
            "stableBorrowRate": "0.04",
        }
        for i in range(10)
    ]


@pytest.fixture
def seed_aave_raw(minio_client, sample_aave_records):
    """Upload sample Aave records to MinIO raw zone."""
    ndjson = "\n".join(json.dumps(r) for r in sample_aave_records)
    minio_client.put_object(
        Bucket="lakehouse",
        Key="raw/aave/date=2024-01-01/hour=00/data.json",
        Body=ndjson.encode("utf-8"),
        ContentType="application/x-ndjson",
    )
    yield
    # Cleanup
    minio_client.delete_object(
        Bucket="lakehouse",
        Key="raw/aave/date=2024-01-01/hour=00/data.json",
    )
