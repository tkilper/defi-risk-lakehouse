"""
Unit tests for health factor computation logic.

These tests verify the core formulas used in dbt models and the Spark
silver transformer without requiring any external services.
"""

import pytest

# ---------------------------------------------------------------------------
# Pure-Python implementation of the health factor formula
# (mirrors the dbt macro and Spark job logic)
# ---------------------------------------------------------------------------


def health_factor(collateral_usd: float, liq_threshold: float, debt_usd: float) -> float | None:
    """HF = (collateral_usd × liq_threshold) / debt_usd"""
    if debt_usd <= 0:
        return None
    return (collateral_usd * liq_threshold) / debt_usd


def classify_risk_tier(hf: float | None) -> str:
    if hf is None:
        return "NO_DEBT"
    if hf < 1.00:
        return "LIQUIDATABLE"
    if hf < 1.05:
        return "CRITICAL"
    if hf < 1.10:
        return "AT_RISK"
    if hf < 1.25:
        return "WATCH"
    return "HEALTHY"


def shocked_health_factor(
    collateral_usd: float,
    liq_threshold: float,
    debt_usd: float,
    shock_pct: float,
) -> float | None:
    """HF after applying a price shock to collateral."""
    return health_factor(collateral_usd * (1.0 - shock_pct), liq_threshold, debt_usd)


def collateral_buffer(collateral_usd: float, liq_threshold: float, debt_usd: float) -> float:
    """USD amount collateral can drop before HF = 1."""
    return collateral_usd - (debt_usd / liq_threshold)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestHealthFactor:
    def test_healthy_position(self):
        # 10k collateral, 80% threshold, 5k debt → HF = (10000 * 0.8) / 5000 = 1.6
        hf = health_factor(10_000, 0.80, 5_000)
        assert hf == pytest.approx(1.6)

    def test_exactly_at_liquidation_boundary(self):
        # HF = 1.0 exactly
        hf = health_factor(10_000, 0.80, 8_000)
        assert hf == pytest.approx(1.0)

    def test_liquidatable_position(self):
        hf = health_factor(5_000, 0.80, 5_000)
        assert hf == pytest.approx(0.8)
        assert hf < 1.0

    def test_zero_debt_returns_none(self):
        hf = health_factor(10_000, 0.80, 0)
        assert hf is None

    def test_zero_collateral(self):
        # Zero collateral with non-zero debt → HF = 0 (maximally liquidatable)
        hf = health_factor(0, 0.80, 5_000)
        assert hf == pytest.approx(0.0)

    def test_threshold_of_one(self):
        # liq_threshold = 1.0 means 100% of collateral counts
        hf = health_factor(10_000, 1.0, 8_000)
        assert hf == pytest.approx(1.25)

    def test_high_collateralisation(self):
        # Very safe position (common in MakerDAO with 150% min ratio)
        hf = health_factor(15_000, 0.667, 5_000)
        assert hf > 1.5


class TestRiskTierClassification:
    @pytest.mark.parametrize(
        "hf,expected",
        [
            (None, "NO_DEBT"),
            (0.50, "LIQUIDATABLE"),
            (0.99, "LIQUIDATABLE"),
            (1.00, "CRITICAL"),  # boundary: exactly 1.0 is CRITICAL (not liquidatable yet)
            (1.04, "CRITICAL"),
            (1.05, "AT_RISK"),
            (1.09, "AT_RISK"),
            (1.10, "WATCH"),
            (1.24, "WATCH"),
            (1.25, "HEALTHY"),
            (2.00, "HEALTHY"),
            (100.0, "HEALTHY"),
        ],
    )
    def test_tiers(self, hf, expected):
        assert classify_risk_tier(hf) == expected


class TestShockedHealthFactor:
    def test_20pct_shock(self):
        # 10k collateral, 80% threshold, 5k debt, -20% shock
        # HF = (10000 * 0.8 * 0.8) / 5000 = 1.28
        hf = shocked_health_factor(10_000, 0.80, 5_000, 0.20)
        assert hf == pytest.approx(1.28)

    def test_50pct_shock_forces_liquidation(self):
        # Position healthy at current prices, liquidatable after -50% shock
        baseline_hf = health_factor(10_000, 0.80, 7_000)
        assert baseline_hf > 1.0, "precondition: healthy at current prices"

        shocked_hf = shocked_health_factor(10_000, 0.80, 7_000, 0.50)
        assert shocked_hf < 1.0, "expected liquidatable after -50% shock"

    def test_0pct_shock_equals_baseline(self):
        baseline = health_factor(10_000, 0.80, 5_000)
        shocked = shocked_health_factor(10_000, 0.80, 5_000, 0.0)
        assert shocked == pytest.approx(baseline)

    def test_100pct_shock_zeroes_collateral(self):
        shocked = shocked_health_factor(10_000, 0.80, 5_000, 1.0)
        assert shocked == pytest.approx(0.0)

    def test_zero_debt_returns_none(self):
        shocked = shocked_health_factor(10_000, 0.80, 0, 0.20)
        assert shocked is None


class TestCollateralBuffer:
    def test_positive_buffer_healthy_position(self):
        # 10k collateral, 80% threshold, 5k debt
        # buffer = 10000 - (5000 / 0.8) = 10000 - 6250 = 3750
        buf = collateral_buffer(10_000, 0.80, 5_000)
        assert buf == pytest.approx(3_750)

    def test_zero_buffer_at_liquidation_boundary(self):
        buf = collateral_buffer(10_000, 0.80, 8_000)
        assert buf == pytest.approx(0.0)

    def test_negative_buffer_liquidatable_position(self):
        buf = collateral_buffer(5_000, 0.80, 5_000)
        assert buf < 0

    def test_buffer_equals_drop_magnitude_to_liquidation(self):
        collateral = 10_000.0
        liq_threshold = 0.80
        debt = 5_000.0
        buf = collateral_buffer(collateral, liq_threshold, debt)
        # Applying a price drop of exactly buffer/collateral should give HF = 1.0
        drop_pct = buf / collateral
        hf = shocked_health_factor(collateral, liq_threshold, debt, drop_pct)
        assert hf == pytest.approx(1.0, abs=1e-9)


class TestMakerDAOLiquidationThreshold:
    """
    MakerDAO uses a liquidation RATIO (minimum collateralisation ratio).
    The Spark transformer converts this to a threshold = 1 / liquidationRatio
    so it's mathematically compatible with the Aave HF formula.
    """

    def test_eth_a_liquidation_ratio_150pct(self):
        # ETH-A: liquidationRatio = 1.5 → threshold = 1/1.5 ≈ 0.667
        liq_ratio = 1.5
        threshold = 1.0 / liq_ratio
        assert threshold == pytest.approx(0.667, abs=0.001)

        # A vault at exactly 150% CR should have HF = 1.0
        # CR = collateral_usd / debt_usd = 1.5
        # HF = collateral_usd * (1/liq_ratio) / debt_usd = 1.5 * 0.667 / 1 ≈ 1.0
        hf = health_factor(1_500, threshold, 1_000)
        assert hf == pytest.approx(1.0, abs=0.001)

    def test_wbtc_b_liquidation_ratio_130pct(self):
        liq_ratio = 1.30
        threshold = 1.0 / liq_ratio
        # Vault at 130% CR → HF should be exactly 1.0
        hf = health_factor(1_300, threshold, 1_000)
        assert hf == pytest.approx(1.0, abs=0.001)
