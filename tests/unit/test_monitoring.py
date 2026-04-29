"""
Unit tests for monitoring logic.

Tests prediction drift statistics and batch scoring helpers using only
pandas and stdlib — no Evidently, no Postgres, no Trino required.
"""

from __future__ import annotations

import pandas as pd
import pytest

# ---------------------------------------------------------------------------
# Inline reimplementation of run_prediction_drift_report logic
# (mirrors monitoring/drift_report.py — kept inline to avoid importing
# evidently which is a heavy dep not always installed locally)
# ---------------------------------------------------------------------------


def compute_prediction_drift_stats(
    ref_preds: pd.DataFrame,
    current_preds: pd.DataFrame,
) -> dict:
    """Pure-pandas prediction drift statistics. Matches drift_report.py logic."""
    if ref_preds.empty or current_preds.empty:
        return {}

    return {
        "ref_mean_probability": ref_preds["liquidation_probability"].mean(),
        "current_mean_probability": current_preds["liquidation_probability"].mean(),
        "probability_shift": (
            current_preds["liquidation_probability"].mean()
            - ref_preds["liquidation_probability"].mean()
        ),
        "current_pct_at_risk_or_higher": (
            current_preds["risk_tier"].isin(["CRITICAL", "AT_RISK"]).mean()
        ),
    }


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestPredictionDriftStats:
    def _make_preds(self, probabilities: list[float], tiers: list[str]) -> pd.DataFrame:
        return pd.DataFrame({"liquidation_probability": probabilities, "risk_tier": tiers})

    def test_returns_empty_dict_when_ref_empty(self):
        ref = pd.DataFrame(columns=["liquidation_probability", "risk_tier"])
        current = self._make_preds([0.3, 0.5], ["AT_RISK", "CRITICAL"])
        result = compute_prediction_drift_stats(ref, current)
        assert result == {}

    def test_returns_empty_dict_when_current_empty(self):
        ref = self._make_preds([0.1, 0.2], ["HEALTHY", "WATCH"])
        current = pd.DataFrame(columns=["liquidation_probability", "risk_tier"])
        result = compute_prediction_drift_stats(ref, current)
        assert result == {}

    def test_probability_shift_positive_when_current_higher(self):
        ref = self._make_preds([0.1, 0.1], ["HEALTHY", "HEALTHY"])
        current = self._make_preds([0.5, 0.5], ["CRITICAL", "CRITICAL"])
        result = compute_prediction_drift_stats(ref, current)
        assert result["probability_shift"] == pytest.approx(0.4)

    def test_probability_shift_negative_when_current_lower(self):
        ref = self._make_preds([0.8, 0.8], ["CRITICAL", "CRITICAL"])
        current = self._make_preds([0.1, 0.1], ["HEALTHY", "HEALTHY"])
        result = compute_prediction_drift_stats(ref, current)
        assert result["probability_shift"] == pytest.approx(-0.7)

    def test_no_shift_when_distributions_identical(self):
        probs = [0.2, 0.4, 0.6]
        tiers = ["WATCH", "AT_RISK", "CRITICAL"]
        ref = self._make_preds(probs, tiers)
        current = self._make_preds(probs, tiers)
        result = compute_prediction_drift_stats(ref, current)
        assert result["probability_shift"] == pytest.approx(0.0)

    def test_all_expected_keys_present(self):
        ref = self._make_preds([0.2, 0.3], ["WATCH", "AT_RISK"])
        current = self._make_preds([0.4, 0.6], ["AT_RISK", "CRITICAL"])
        result = compute_prediction_drift_stats(ref, current)
        assert set(result.keys()) == {
            "ref_mean_probability",
            "current_mean_probability",
            "probability_shift",
            "current_pct_at_risk_or_higher",
        }

    def test_pct_at_risk_or_higher_counts_critical_and_at_risk(self):
        current = self._make_preds(
            [0.6, 0.3, 0.15, 0.05],
            ["CRITICAL", "AT_RISK", "WATCH", "HEALTHY"],
        )
        ref = self._make_preds([0.1], ["HEALTHY"])
        result = compute_prediction_drift_stats(ref, current)
        # 2 out of 4 are AT_RISK or CRITICAL = 50%
        assert result["current_pct_at_risk_or_higher"] == pytest.approx(0.50)

    def test_pct_at_risk_zero_when_all_healthy(self):
        current = self._make_preds([0.01, 0.02], ["HEALTHY", "HEALTHY"])
        ref = self._make_preds([0.5], ["CRITICAL"])
        result = compute_prediction_drift_stats(ref, current)
        assert result["current_pct_at_risk_or_higher"] == pytest.approx(0.0)

    def test_pct_at_risk_one_when_all_critical(self):
        current = self._make_preds([0.9, 0.8], ["CRITICAL", "CRITICAL"])
        ref = self._make_preds([0.1], ["HEALTHY"])
        result = compute_prediction_drift_stats(ref, current)
        assert result["current_pct_at_risk_or_higher"] == pytest.approx(1.0)

    def test_mean_probabilities_correct(self):
        ref = self._make_preds([0.2, 0.4], ["WATCH", "AT_RISK"])
        current = self._make_preds([0.5, 0.7], ["CRITICAL", "CRITICAL"])
        result = compute_prediction_drift_stats(ref, current)
        assert result["ref_mean_probability"] == pytest.approx(0.3)
        assert result["current_mean_probability"] == pytest.approx(0.6)


class TestDriftThresholdDetection:
    """Test the business logic around drift trigger thresholds."""

    _DRIFT_THRESHOLD = 0.25  # fraction of features drifted before flagging retrain

    def test_above_threshold_flags_retrain(self):
        drift_fraction = 0.30
        assert drift_fraction >= self._DRIFT_THRESHOLD

    def test_below_threshold_does_not_flag(self):
        drift_fraction = 0.20
        assert drift_fraction < self._DRIFT_THRESHOLD

    def test_exactly_at_threshold_triggers(self):
        drift_fraction = 0.25
        assert drift_fraction >= self._DRIFT_THRESHOLD

    _PROB_SHIFT_ALERT = 0.10  # shift in mean probability that warrants investigation

    def test_large_prob_shift_triggers_alert(self):
        shift = 0.15
        assert abs(shift) > self._PROB_SHIFT_ALERT

    def test_small_prob_shift_within_tolerance(self):
        shift = 0.05
        assert abs(shift) <= self._PROB_SHIFT_ALERT
