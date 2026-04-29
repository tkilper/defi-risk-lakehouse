"""
Unit tests for feature engineering logic.

These tests run without Docker — they test the Python logic directly
without invoking Spark or any database.
"""

from __future__ import annotations

import math
from datetime import UTC, datetime, timedelta

import pytest
from features.feature_definitions import ALL_FEATURES, LABEL_COL, get_risk_tier

# ---------------------------------------------------------------------------
# Feature definitions
# ---------------------------------------------------------------------------


class TestRiskTierMapping:
    def test_critical_at_0_6(self):
        assert get_risk_tier(0.6) == "CRITICAL"

    def test_critical_at_exact_threshold(self):
        assert get_risk_tier(0.50) == "CRITICAL"

    def test_at_risk_below_critical(self):
        assert get_risk_tier(0.3) == "AT_RISK"

    def test_watch(self):
        assert get_risk_tier(0.15) == "WATCH"

    def test_healthy_below_watch(self):
        assert get_risk_tier(0.05) == "HEALTHY"

    def test_healthy_at_zero(self):
        assert get_risk_tier(0.0) == "HEALTHY"


class TestAllFeaturesList:
    def test_no_duplicates(self):
        assert len(ALL_FEATURES) == len(set(ALL_FEATURES))

    def test_health_factor_present(self):
        assert "health_factor" in ALL_FEATURES

    def test_label_not_in_features(self):
        assert LABEL_COL not in ALL_FEATURES


# ---------------------------------------------------------------------------
# Health factor delta computation
# ---------------------------------------------------------------------------


class TestHealthFactorDelta:
    def _compute_delta(self, hf_now: float, hf_earlier: float) -> float:
        return hf_now - hf_earlier

    def test_delta_is_current_minus_previous(self):
        """HF delta = current HF - HF 6h ago (positive = improving)."""
        delta = self._compute_delta(hf_now=1.20, hf_earlier=1.10)
        assert abs(delta - 0.10) < 1e-9

    def test_negative_delta_signals_deterioration(self):
        delta = self._compute_delta(hf_now=1.05, hf_earlier=1.20)
        assert delta < 0

    def test_zero_delta_when_unchanged(self):
        delta = self._compute_delta(hf_now=1.15, hf_earlier=1.15)
        assert delta == 0.0


# ---------------------------------------------------------------------------
# Concentration features
# ---------------------------------------------------------------------------


class TestConcentrationFeatures:
    def test_single_asset_largest_pct_is_one(self):
        """Position with one collateral asset must have largest_collateral_pct == 1.0."""
        num_assets = 1
        largest_pct = 1.0 / num_assets  # trivial: 100%
        assert largest_pct == pytest.approx(1.0)

    def test_two_equal_assets(self):
        collateral = {"WETH": 5000.0, "WBTC": 5000.0}
        total = sum(collateral.values())
        largest_pct = max(v / total for v in collateral.values())
        assert largest_pct == pytest.approx(0.50)

    def test_dominant_asset_threshold(self):
        """ETH is dominant when it exceeds 60% of collateral by value."""
        collateral = {"WETH": 7000.0, "USDC": 3000.0}
        total = sum(collateral.values())
        eth_pct = collateral["WETH"] / total
        assert eth_pct > 0.60  # dominant


# ---------------------------------------------------------------------------
# Price change pct edge cases
# ---------------------------------------------------------------------------


class TestPriceChangePct:
    def _price_change_pct(self, current: float, previous: float) -> float | None:
        """Compute price change percentage; return None if previous price is 0."""
        if previous == 0:
            return None
        return (current - previous) / previous * 100.0

    def test_positive_change(self):
        result = self._price_change_pct(current=2200.0, previous=2000.0)
        assert result == pytest.approx(10.0)

    def test_negative_change(self):
        result = self._price_change_pct(current=1800.0, previous=2000.0)
        assert result == pytest.approx(-10.0)

    def test_zero_previous_returns_none_not_inf(self):
        """Zero previous price should return None, not raise ZeroDivisionError or return inf."""
        result = self._price_change_pct(current=100.0, previous=0.0)
        assert result is None
        # Explicitly assert it's not infinity
        assert result != math.inf


# ---------------------------------------------------------------------------
# Label assignment timing
# ---------------------------------------------------------------------------


class TestLabelTiming:
    def test_snapshot_within_24h_should_not_be_labeled_negative(self):
        """
        Snapshots within 24h of NOW must be excluded from training —
        they cannot yet have a confirmed negative label.
        """
        now = datetime.now(tz=UTC)
        recent_snapshot = now - timedelta(hours=12)
        cutoff = now - timedelta(hours=24)

        should_include = recent_snapshot <= cutoff
        assert not should_include, (
            "A snapshot from 12h ago should NOT be included in labeled training data"
        )

    def test_snapshot_older_than_24h_can_be_labeled(self):
        now = datetime.now(tz=UTC)
        old_snapshot = now - timedelta(hours=48)
        cutoff = now - timedelta(hours=24)

        should_include = old_snapshot <= cutoff
        assert should_include, (
            "A snapshot from 48h ago should be includeable in training data"
        )
