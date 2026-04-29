"""
Integration tests for the FastAPI serving endpoint.

Requires the `api` Docker Compose service to be running with a loaded model.
Run with: pytest tests/integration/ -m integration -v

These tests validate the full request/response cycle against the live API container.
"""

from __future__ import annotations

import os
from typing import Any

import pytest
import requests

API_BASE = os.getenv("API_URL", "http://localhost:8000")


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def valid_position() -> dict[str, Any]:
    """A minimal valid feature vector for a high-risk position."""
    return {
        "health_factor": 1.04,
        "collateral_usd": 10000.0,
        "debt_usd": 9200.0,
        "ltv_ratio": 0.92,
        "liquidation_threshold": 0.80,
        "num_collateral_assets": 1,
        "largest_collateral_pct": 1.0,
        "is_eth_dominant_collateral": 1,
        "protocol_encoded": 0,
        "user_prior_liquidations": 0,
    }


@pytest.fixture(scope="session")
def healthy_position() -> dict[str, Any]:
    """A feature vector for a healthy, low-risk position."""
    return {
        "health_factor": 2.50,
        "collateral_usd": 50000.0,
        "debt_usd": 10000.0,
        "ltv_ratio": 0.20,
        "liquidation_threshold": 0.80,
        "health_factor_delta_6h": 0.05,
        "num_collateral_assets": 2,
        "largest_collateral_pct": 0.60,
        "is_eth_dominant_collateral": 1,
        "protocol_encoded": 0,
        "user_prior_liquidations": 0,
    }


# ---------------------------------------------------------------------------
# /health endpoint
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestHealthEndpoint:
    def test_returns_200(self):
        resp = requests.get(f"{API_BASE}/health", timeout=10)
        assert resp.status_code == 200

    def test_response_schema(self):
        resp = requests.get(f"{API_BASE}/health", timeout=10)
        body = resp.json()
        assert "status" in body
        assert "model_version" in body
        assert "loaded_at" in body

    def test_status_is_ok(self):
        resp = requests.get(f"{API_BASE}/health", timeout=10)
        body = resp.json()
        assert body["status"] == "ok", f"Model not loaded: {body}"


# ---------------------------------------------------------------------------
# /predict endpoint
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestPredictEndpoint:
    def test_valid_position_returns_200(self, valid_position):
        resp = requests.post(f"{API_BASE}/predict", json=valid_position, timeout=10)
        assert resp.status_code == 200

    def test_response_has_probability_field(self, valid_position):
        resp = requests.post(f"{API_BASE}/predict", json=valid_position, timeout=10)
        body = resp.json()
        assert "liquidation_probability" in body

    def test_probability_is_between_0_and_1(self, valid_position):
        resp = requests.post(f"{API_BASE}/predict", json=valid_position, timeout=10)
        prob = resp.json()["liquidation_probability"]
        assert 0.0 <= prob <= 1.0

    def test_risk_tier_in_response(self, valid_position):
        resp = requests.post(f"{API_BASE}/predict", json=valid_position, timeout=10)
        body = resp.json()
        assert body["risk_tier"] in ("CRITICAL", "AT_RISK", "WATCH", "HEALTHY")

    def test_model_version_in_response(self, valid_position):
        resp = requests.post(f"{API_BASE}/predict", json=valid_position, timeout=10)
        body = resp.json()
        assert "model_version" in body
        assert body["model_version"] != ""

    def test_model_version_matches_registry(self, valid_position):
        """Assert returned model_version matches the Production version in MLflow."""
        import mlflow

        mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000"))
        client = mlflow.MlflowClient()
        versions = client.get_latest_versions("liquidation-predictor", stages=["Production"])

        if not versions:
            pytest.skip("No Production model in registry — cannot verify version.")

        expected_version = versions[0].version
        resp = requests.post(f"{API_BASE}/predict", json=valid_position, timeout=10)
        actual_version = resp.json()["model_version"]
        assert actual_version == expected_version

    def test_high_health_factor_not_critical(self, healthy_position):
        """A position with HF=2.5 should not be predicted as CRITICAL."""
        resp = requests.post(f"{API_BASE}/predict", json=healthy_position, timeout=10)
        body = resp.json()
        assert body["risk_tier"] != "CRITICAL", (
            f"Healthy position unexpectedly predicted as CRITICAL: prob={body['liquidation_probability']}"
        )


# ---------------------------------------------------------------------------
# /predict input validation
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestPredictValidation:
    def test_missing_required_field_returns_422(self):
        """Omitting health_factor (required) should return 422 Unprocessable Entity."""
        incomplete = {
            "collateral_usd": 10000.0,
            "debt_usd": 8000.0,
            # health_factor missing
        }
        resp = requests.post(f"{API_BASE}/predict", json=incomplete, timeout=10)
        assert resp.status_code == 422

    def test_string_health_factor_returns_422(self):
        """Sending health_factor as a string should return 422."""
        bad_payload = {
            "health_factor": "not-a-number",
            "collateral_usd": 10000.0,
            "debt_usd": 8000.0,
        }
        resp = requests.post(f"{API_BASE}/predict", json=bad_payload, timeout=10)
        assert resp.status_code == 422

    def test_negative_health_factor_returns_422(self):
        bad_payload = {
            "health_factor": -1.0,
            "collateral_usd": 10000.0,
            "debt_usd": 8000.0,
        }
        resp = requests.post(f"{API_BASE}/predict", json=bad_payload, timeout=10)
        assert resp.status_code == 422


# ---------------------------------------------------------------------------
# /metrics endpoint
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestMetricsEndpoint:
    def test_returns_200(self):
        resp = requests.get(f"{API_BASE}/metrics", timeout=10)
        assert resp.status_code == 200

    def test_prometheus_format(self):
        resp = requests.get(f"{API_BASE}/metrics", timeout=10)
        assert "predict_requests_total" in resp.text
