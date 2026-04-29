"""
Feature definitions and constants for the liquidation predictor.

All feature column names are defined here so that the feature pipeline,
training code, and serving layer stay in sync.
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Base features (passthrough from silver — already computed in silver layer)
# ---------------------------------------------------------------------------
FEAT_HEALTH_FACTOR = "health_factor"
FEAT_COLLATERAL_USD = "collateral_usd"
FEAT_DEBT_USD = "debt_usd"
FEAT_LTV_RATIO = "ltv_ratio"
FEAT_LIQUIDATION_THRESHOLD = "liquidation_threshold"

# ---------------------------------------------------------------------------
# Price velocity features (require price snapshots at multiple timestamps)
# ---------------------------------------------------------------------------
FEAT_ETH_PRICE_CHANGE_1H = "eth_price_change_pct_1h"
FEAT_ETH_PRICE_CHANGE_6H = "eth_price_change_pct_6h"
FEAT_ETH_PRICE_CHANGE_24H = "eth_price_change_pct_24h"
FEAT_BTC_PRICE_CHANGE_24H = "btc_price_change_pct_24h"

# ---------------------------------------------------------------------------
# Health factor trend features (computed from position_snapshots window)
# ---------------------------------------------------------------------------
FEAT_HF_DELTA_6H = "health_factor_delta_6h"
FEAT_HF_DELTA_24H = "health_factor_delta_24h"
FEAT_HF_MIN_24H = "health_factor_min_24h"

# ---------------------------------------------------------------------------
# Position structure features
# ---------------------------------------------------------------------------
FEAT_NUM_COLLATERAL_ASSETS = "num_collateral_assets"
FEAT_LARGEST_COLLATERAL_PCT = "largest_collateral_pct"
FEAT_IS_ETH_DOMINANT = "is_eth_dominant_collateral"
FEAT_COLLATERAL_VOLATILITY_30D = "collateral_volatility_30d"

# ---------------------------------------------------------------------------
# Protocol features
# ---------------------------------------------------------------------------
FEAT_PROTOCOL = "protocol_encoded"
FEAT_BORROW_APY = "borrow_apy"

# ---------------------------------------------------------------------------
# User history features
# ---------------------------------------------------------------------------
FEAT_USER_PRIOR_LIQUIDATIONS = "user_prior_liquidations"
FEAT_DAYS_POSITION_OPEN = "days_position_open"

# ---------------------------------------------------------------------------
# Label
# ---------------------------------------------------------------------------
LABEL_COL = "liquidated_within_24h"

# ---------------------------------------------------------------------------
# All feature columns in training order
# Changing this order will break saved models — update serving/predictor.py too
# ---------------------------------------------------------------------------
ALL_FEATURES: list[str] = [
    FEAT_HEALTH_FACTOR,
    FEAT_COLLATERAL_USD,
    FEAT_DEBT_USD,
    FEAT_LTV_RATIO,
    FEAT_LIQUIDATION_THRESHOLD,
    FEAT_ETH_PRICE_CHANGE_1H,
    FEAT_ETH_PRICE_CHANGE_6H,
    FEAT_ETH_PRICE_CHANGE_24H,
    FEAT_BTC_PRICE_CHANGE_24H,
    FEAT_HF_DELTA_6H,
    FEAT_HF_DELTA_24H,
    FEAT_HF_MIN_24H,
    FEAT_NUM_COLLATERAL_ASSETS,
    FEAT_LARGEST_COLLATERAL_PCT,
    FEAT_IS_ETH_DOMINANT,
    FEAT_COLLATERAL_VOLATILITY_30D,
    FEAT_PROTOCOL,
    FEAT_BORROW_APY,
    FEAT_USER_PRIOR_LIQUIDATIONS,
    FEAT_DAYS_POSITION_OPEN,
]

# Protocol label encoding — must match training
PROTOCOL_ENCODING: dict[str, int] = {
    "aave_v3": 0,
    "compound_v3": 1,
    "maker": 2,
}

# Risk tier thresholds (mirrors the existing dbt fct_liquidation_risk tiers)
RISK_TIERS: list[tuple[float, str]] = [
    (0.50, "CRITICAL"),
    (0.25, "AT_RISK"),
    (0.10, "WATCH"),
    (0.00, "HEALTHY"),
]


def get_risk_tier(probability: float) -> str:
    """Map a predicted probability to a risk tier label."""
    for threshold, tier in RISK_TIERS:
        if probability >= threshold:
            return tier
    return "HEALTHY"
