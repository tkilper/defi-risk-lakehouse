"""
Model predictor — wraps MLflow model loading and inference.

Separates model lifecycle concerns (loading, hot-reloading) from the
FastAPI routing layer in app.py.
"""

from __future__ import annotations

import logging
import os
from datetime import UTC, datetime
from typing import Any

import pandas as pd
from pydantic import BaseModel, Field, field_validator

logger = logging.getLogger(__name__)

MODEL_REGISTRY_NAME = os.getenv("MODEL_NAME", "liquidation-predictor")
MODEL_STAGE = os.getenv("MODEL_STAGE", "Production")
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")

# Feature columns in the exact order the model was trained on.
# Must stay in sync with features/feature_definitions.py:ALL_FEATURES.
FEATURE_COLUMNS: list[str] = [
    "health_factor",
    "collateral_usd",
    "debt_usd",
    "ltv_ratio",
    "liquidation_threshold",
    "eth_price_change_pct_1h",
    "eth_price_change_pct_6h",
    "eth_price_change_pct_24h",
    "btc_price_change_pct_24h",
    "health_factor_delta_6h",
    "health_factor_delta_24h",
    "health_factor_min_24h",
    "num_collateral_assets",
    "largest_collateral_pct",
    "is_eth_dominant_collateral",
    "collateral_volatility_30d",
    "protocol_encoded",
    "borrow_apy",
    "user_prior_liquidations",
    "days_position_open",
]

# Risk tier thresholds — mirrors feature_definitions.py:RISK_TIERS
_RISK_TIERS: list[tuple[float, str]] = [
    (0.50, "CRITICAL"),
    (0.25, "AT_RISK"),
    (0.10, "WATCH"),
    (0.00, "HEALTHY"),
]


def get_risk_tier(probability: float) -> str:
    for threshold, tier in _RISK_TIERS:
        if probability >= threshold:
            return tier
    return "HEALTHY"


# ---------------------------------------------------------------------------
# Input / output schemas
# ---------------------------------------------------------------------------


class PositionFeatures(BaseModel):
    """
    Feature vector for a single DeFi borrow position.

    All monetary values are in USD. Optional fields default to None
    (XGBoost handles missing values natively via its missing=-999 sentinel).
    """

    health_factor: float = Field(..., gt=0, description="Current health factor")
    collateral_usd: float = Field(..., ge=0, description="Total collateral in USD")
    debt_usd: float = Field(..., ge=0, description="Total debt in USD")
    ltv_ratio: float | None = Field(None, description="debt_usd / collateral_usd")
    liquidation_threshold: float | None = Field(None, ge=0, le=1)

    eth_price_change_pct_1h: float | None = None
    eth_price_change_pct_6h: float | None = None
    eth_price_change_pct_24h: float | None = None
    btc_price_change_pct_24h: float | None = None

    health_factor_delta_6h: float | None = None
    health_factor_delta_24h: float | None = None
    health_factor_min_24h: float | None = None

    num_collateral_assets: int = Field(default=1, ge=1)
    largest_collateral_pct: float = Field(default=1.0, ge=0, le=1)
    is_eth_dominant_collateral: int = Field(default=0, ge=0, le=1)
    collateral_volatility_30d: float | None = None

    protocol_encoded: int = Field(default=0, ge=-1, le=2)
    borrow_apy: float | None = None

    user_prior_liquidations: int = Field(default=0, ge=0)
    days_position_open: float | None = None

    @field_validator("health_factor")
    @classmethod
    def health_factor_must_be_positive(cls, v: float) -> float:
        if v <= 0:
            raise ValueError("health_factor must be > 0")
        return v


class PredictionResponse(BaseModel):
    liquidation_probability: float
    risk_tier: str
    model_version: str
    scored_at: str


# ---------------------------------------------------------------------------
# Predictor
# ---------------------------------------------------------------------------


class LiquidationPredictor:
    """Loads and caches the XGBoost model from the MLflow Model Registry."""

    def __init__(self) -> None:
        self._model: Any = None
        self._model_version: str = "unknown"
        self._loaded_at: str = ""

    def load(self) -> None:
        """Load (or reload) the Production model from the MLflow registry."""
        import mlflow  # lazy — only required in the serving container
        import mlflow.xgboost  # noqa: F401

        mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
        uri = f"models:/{MODEL_REGISTRY_NAME}/{MODEL_STAGE}"
        logger.info("Loading model from %s ...", uri)

        self._model = mlflow.xgboost.load_model(uri)

        # Retrieve the version number from the registry
        client = mlflow.MlflowClient()
        versions = client.get_latest_versions(MODEL_REGISTRY_NAME, stages=[MODEL_STAGE])
        self._model_version = versions[0].version if versions else "unknown"
        self._loaded_at = datetime.now(tz=UTC).isoformat()

        logger.info(
            "Model loaded: %s v%s (loaded at %s)",
            MODEL_REGISTRY_NAME,
            self._model_version,
            self._loaded_at,
        )

    @property
    def is_loaded(self) -> bool:
        return self._model is not None

    @property
    def model_version(self) -> str:
        return self._model_version

    @property
    def loaded_at(self) -> str:
        return self._loaded_at

    def predict(self, features: PositionFeatures) -> PredictionResponse:
        if not self.is_loaded:
            raise RuntimeError("Model not loaded. Call load() first.")

        # Build feature row — use -999.0 as the XGBoost missing sentinel
        row: dict[str, float] = {}
        for col in FEATURE_COLUMNS:
            val = getattr(features, col, None)
            row[col] = float(val) if val is not None else -999.0

        X = pd.DataFrame([row], columns=FEATURE_COLUMNS)
        prob = float(self._model.predict_proba(X)[0, 1])
        prob = max(0.0, min(1.0, prob))  # clamp to [0, 1]

        return PredictionResponse(
            liquidation_probability=prob,
            risk_tier=get_risk_tier(prob),
            model_version=self._model_version,
            scored_at=datetime.now(tz=UTC).isoformat(),
        )
