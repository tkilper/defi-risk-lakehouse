"""
FastAPI serving endpoint for the DeFi liquidation predictor.

Endpoints:
  POST /predict     — score a single position; returns liquidation probability + risk tier
  GET  /health      — liveness check; returns model version and load time
  GET  /metrics     — Prometheus-compatible text metrics
  POST /reload      — hot-reload the model from the MLflow registry (no restart needed)

Usage:
    uvicorn serving.app:app --host 0.0.0.0 --port 8000
"""

from __future__ import annotations

import logging
import os
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from fastapi.responses import PlainTextResponse

from serving.predictor import LiquidationPredictor, PositionFeatures, PredictionResponse

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Global predictor instance
# ---------------------------------------------------------------------------

predictor = LiquidationPredictor()

# Prometheus-style counters (simple in-memory; replace with prometheus_client
# if you need a proper scrape endpoint)
_metrics: dict[str, float] = {
    "predict_requests_total": 0,
    "predict_errors_total": 0,
    "predict_latency_seconds_sum": 0.0,
    "predict_critical_total": 0,
    "predict_at_risk_total": 0,
}


# ---------------------------------------------------------------------------
# Startup
# ---------------------------------------------------------------------------


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        predictor.load()
    except Exception:
        logger.exception("Failed to load model on startup — /predict will return 503 until /reload succeeds.")
    yield


app = FastAPI(
    title="DeFi Liquidation Predictor",
    description="Predicts the probability that a DeFi borrow position is liquidated within 24 hours.",
    version="1.0.0",
    lifespan=lifespan,
)


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


@app.post("/predict", response_model=PredictionResponse)
def predict(features: PositionFeatures) -> PredictionResponse:
    """
    Score a single borrow position.

    Returns the liquidation probability (0–1) and a risk tier label.
    """
    if not predictor.is_loaded:
        raise HTTPException(status_code=503, detail="Model not loaded. Try again shortly or POST /reload.")

    _metrics["predict_requests_total"] += 1
    t0 = time.perf_counter()

    try:
        result = predictor.predict(features)
    except Exception as exc:
        _metrics["predict_errors_total"] += 1
        logger.exception("Prediction failed.")
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    elapsed = time.perf_counter() - t0
    _metrics["predict_latency_seconds_sum"] += elapsed

    if result.risk_tier == "CRITICAL":
        _metrics["predict_critical_total"] += 1
    elif result.risk_tier == "AT_RISK":
        _metrics["predict_at_risk_total"] += 1

    return result


@app.get("/health")
def health() -> dict:
    """Liveness check. Returns model version and load timestamp."""
    return {
        "status": "ok" if predictor.is_loaded else "degraded",
        "model_name": os.getenv("MODEL_NAME", "liquidation-predictor"),
        "model_stage": os.getenv("MODEL_STAGE", "Production"),
        "model_version": predictor.model_version,
        "loaded_at": predictor.loaded_at,
    }


@app.get("/metrics", response_class=PlainTextResponse)
def metrics() -> str:
    """Prometheus-format metrics."""
    lines = []
    for name, value in _metrics.items():
        lines.append(f"# HELP {name} {name.replace('_', ' ')}")
        lines.append(f"# TYPE {name} counter")
        lines.append(f"{name} {value}")
    return "\n".join(lines) + "\n"


@app.post("/reload")
def reload_model() -> dict:
    """
    Hot-reload the Production model from the MLflow registry.
    Called by the Airflow retraining DAG after promoting a new champion.
    """
    try:
        predictor.load()
    except Exception as exc:
        logger.exception("Model reload failed.")
        raise HTTPException(status_code=500, detail=f"Reload failed: {exc}") from exc

    return {
        "status": "reloaded",
        "model_version": predictor.model_version,
        "loaded_at": predictor.loaded_at,
    }
