"""
Unit tests for the serving predictor layer.

Tests use a mocked XGBoost model — no MLflow connection required.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import numpy as np
import pytest
from pydantic import ValidationError
from serving.predictor import (
    LiquidationPredictor,
    PositionFeatures,
    get_risk_tier,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_valid_features(**overrides) -> PositionFeatures:
    """Return a PositionFeatures instance with all required fields populated."""
    defaults = {
        "health_factor": 1.08,
        "collateral_usd": 10_000.0,
        "debt_usd": 8_500.0,
        "ltv_ratio": 0.85,
        "liquidation_threshold": 0.80,
        "num_collateral_assets": 1,
        "largest_collateral_pct": 1.0,
        "is_eth_dominant_collateral": 1,
        "protocol_encoded": 0,
        "user_prior_liquidations": 0,
    }
    defaults.update(overrides)
    return PositionFeatures(**defaults)


def _mock_xgb_model(probability: float):
    """Return a mock XGBoost model that always predicts the given probability."""
    model = MagicMock()
    model.predict_proba.return_value = np.array([[1 - probability, probability]])
    return model


# ---------------------------------------------------------------------------
# Risk tier mapping
# ---------------------------------------------------------------------------


class TestRiskTierMapping:
    def test_probability_0_6_is_critical(self):
        assert get_risk_tier(0.6) == "CRITICAL"

    def test_probability_0_3_is_at_risk(self):
        assert get_risk_tier(0.3) == "AT_RISK"

    def test_probability_0_15_is_watch(self):
        assert get_risk_tier(0.15) == "WATCH"

    def test_probability_0_05_is_healthy(self):
        assert get_risk_tier(0.05) == "HEALTHY"

    def test_exact_boundary_at_0_50_is_critical(self):
        assert get_risk_tier(0.50) == "CRITICAL"

    def test_just_below_critical_is_at_risk(self):
        assert get_risk_tier(0.499) == "AT_RISK"


# ---------------------------------------------------------------------------
# Probability output
# ---------------------------------------------------------------------------


class TestProbabilityOutput:
    def test_predict_returns_float_between_0_and_1(self):
        predictor = LiquidationPredictor()
        predictor._model = _mock_xgb_model(0.73)
        predictor._model_version = "1"

        features = _make_valid_features()
        result = predictor.predict(features)

        assert isinstance(result.liquidation_probability, float)
        assert 0.0 <= result.liquidation_probability <= 1.0

    def test_high_probability_maps_to_critical(self):
        predictor = LiquidationPredictor()
        predictor._model = _mock_xgb_model(0.80)
        predictor._model_version = "2"

        result = predictor.predict(_make_valid_features())
        assert result.risk_tier == "CRITICAL"

    def test_low_probability_maps_to_healthy(self):
        predictor = LiquidationPredictor()
        predictor._model = _mock_xgb_model(0.02)
        predictor._model_version = "2"

        result = predictor.predict(_make_valid_features())
        assert result.risk_tier == "HEALTHY"

    def test_model_version_in_response(self):
        predictor = LiquidationPredictor()
        predictor._model = _mock_xgb_model(0.5)
        predictor._model_version = "42"

        result = predictor.predict(_make_valid_features())
        assert result.model_version == "42"

    def test_probability_clamped_to_0_1(self):
        """Model outputs should be clamped even if the model returns out-of-range values."""
        predictor = LiquidationPredictor()
        # Simulate a model that returns slightly out-of-range probability
        mock_model = MagicMock()
        mock_model.predict_proba.return_value = np.array([[0.0, 1.01]])
        predictor._model = mock_model
        predictor._model_version = "1"

        result = predictor.predict(_make_valid_features())
        assert result.liquidation_probability <= 1.0


# ---------------------------------------------------------------------------
# Validation errors
# ---------------------------------------------------------------------------


class TestInputValidation:
    def test_negative_health_factor_raises(self):
        with pytest.raises(ValidationError):
            _make_valid_features(health_factor=-1.0)

    def test_zero_health_factor_raises(self):
        with pytest.raises(ValidationError):
            _make_valid_features(health_factor=0.0)

    def test_missing_required_field_raises(self):
        with pytest.raises(ValidationError):
            PositionFeatures()  # health_factor is required

    def test_string_health_factor_raises(self):
        with pytest.raises(ValidationError):
            PositionFeatures(health_factor="not-a-number", collateral_usd=0, debt_usd=0)  # type: ignore


# ---------------------------------------------------------------------------
# Model not loaded
# ---------------------------------------------------------------------------


class TestNotLoaded:
    def test_predict_raises_when_not_loaded(self):
        predictor = LiquidationPredictor()
        # model is None by default
        with pytest.raises(RuntimeError, match="not loaded"):
            predictor.predict(_make_valid_features())

    def test_is_loaded_false_before_load(self):
        predictor = LiquidationPredictor()
        assert not predictor.is_loaded
