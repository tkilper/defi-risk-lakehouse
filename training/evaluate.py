"""
Model Evaluation + SHAP Interpretability
==========================================
Loads the Production model from the MLflow registry, runs it against the
holdout set, and generates SHAP interpretability plots.

Artifacts logged to MLflow:
  - shap_summary_plot.png   (global feature importance)
  - shap_beeswarm.png       (feature direction + magnitude)
  - shap_waterfall_example.png (single high-risk position explained)

Usage:
    python training/evaluate.py
    python training/evaluate.py --run-id <mlflow_run_id>
    python training/evaluate.py --model-stage Production
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import matplotlib
import mlflow
import mlflow.xgboost
import numpy as np
import pandas as pd
import shap

matplotlib.use("Agg")  # non-interactive backend for server environments
import matplotlib.pyplot as plt

sys.path.insert(0, str(Path(__file__).parents[1]))
from features.feature_definitions import ALL_FEATURES, LABEL_COL

from training.train import (
    EXPERIMENT_NAME,
    MLFLOW_TRACKING_URI,
    MODEL_REGISTRY_NAME,
    compute_metrics,
    load_features_from_trino,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

_PLOTS_DIR = Path("reports")


def load_model(run_id: str | None = None, stage: str = "Production"):
    """Load model from registry (by stage) or from a specific run."""
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)

    if run_id:
        logger.info("Loading model from run %s...", run_id)
        return mlflow.xgboost.load_model(f"runs:/{run_id}/model")

    uri = f"models:/{MODEL_REGISTRY_NAME}/{stage}"
    logger.info("Loading model from registry: %s", uri)
    return mlflow.xgboost.load_model(uri)


def generate_shap_plots(model, X: pd.DataFrame, run_id: str) -> None:
    """Generate and log SHAP interpretability plots to MLflow."""
    _PLOTS_DIR.mkdir(exist_ok=True)

    logger.info("Computing SHAP values for %d samples...", len(X))
    explainer = shap.TreeExplainer(model)
    shap_values = explainer.shap_values(X)

    # Global summary plot (bar chart of mean |SHAP|)
    fig_summary, ax = plt.subplots(figsize=(10, 8))
    shap.summary_plot(shap_values, X, plot_type="bar", show=False)
    summary_path = _PLOTS_DIR / "shap_summary_plot.png"
    plt.savefig(summary_path, bbox_inches="tight", dpi=150)
    plt.close()
    logger.info("Saved: %s", summary_path)

    # Beeswarm plot (direction + magnitude per feature)
    fig_beeswarm, ax = plt.subplots(figsize=(10, 8))
    shap.summary_plot(shap_values, X, show=False)
    beeswarm_path = _PLOTS_DIR / "shap_beeswarm.png"
    plt.savefig(beeswarm_path, bbox_inches="tight", dpi=150)
    plt.close()
    logger.info("Saved: %s", beeswarm_path)

    # Waterfall for the highest-risk predicted position
    y_prob = model.predict_proba(X)[:, 1]
    highest_risk_idx = np.argmax(y_prob)

    shap_explanation = shap.Explanation(
        values=shap_values[highest_risk_idx],
        base_values=explainer.expected_value,
        data=X.iloc[highest_risk_idx].values,
        feature_names=list(X.columns),
    )
    fig_waterfall, ax = plt.subplots(figsize=(10, 6))
    shap.waterfall_plot(shap_explanation, show=False)
    waterfall_path = _PLOTS_DIR / "shap_waterfall_example.png"
    plt.savefig(waterfall_path, bbox_inches="tight", dpi=150)
    plt.close()
    logger.info("Saved: %s", waterfall_path)

    # Log all plots to the run
    with mlflow.start_run(run_id=run_id):
        mlflow.log_artifact(str(summary_path))
        mlflow.log_artifact(str(beeswarm_path))
        mlflow.log_artifact(str(waterfall_path))

    # Log top feature by SHAP importance
    mean_abs_shap = np.abs(shap_values).mean(axis=0)
    top_feature = X.columns[np.argmax(mean_abs_shap)]
    logger.info("Top feature by mean |SHAP|: %s (%.4f)", top_feature, mean_abs_shap.max())


def evaluate(run_id: str | None = None, model_stage: str = "Production") -> dict:
    """
    Evaluate the model on the holdout period and generate SHAP plots.

    Returns the holdout metrics dict.
    """
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(EXPERIMENT_NAME)

    model = load_model(run_id=run_id, stage=model_stage)

    df = load_features_from_trino()
    if len(df) == 0:
        raise ValueError("No data available for evaluation.")

    # Use the last 14 days as the holdout (consistent with train.py default)
    max_date = pd.to_datetime(df["snapshot_date"]).max()
    holdout_cutoff = max_date - pd.Timedelta(days=14)
    holdout_df = df[pd.to_datetime(df["snapshot_date"]) > holdout_cutoff]

    if len(holdout_df) == 0:
        logger.warning("No holdout data found. Using all available data.")
        holdout_df = df

    X = holdout_df[ALL_FEATURES].fillna(-999.0)
    y = holdout_df[LABEL_COL]

    y_prob = model.predict_proba(X)[:, 1]
    metrics = compute_metrics(y.values, y_prob)

    logger.info("Evaluation metrics on holdout: %s", metrics)

    # Log evaluation metrics
    with mlflow.start_run(
        run_name=f"evaluation-{model_stage}",
        tags={"model_stage": model_stage, "evaluation_rows": str(len(X))},
    ) as eval_run:
        for k, v in metrics.items():
            mlflow.log_metric(f"eval_{k}", v)

        generate_shap_plots(model, X, eval_run.info.run_id)

    return metrics


def main() -> None:
    parser = argparse.ArgumentParser(description="Evaluate the liquidation predictor")
    parser.add_argument("--run-id", type=str, default=None)
    parser.add_argument("--model-stage", type=str, default="Production")
    args = parser.parse_args()

    metrics = evaluate(run_id=args.run_id, model_stage=args.model_stage)
    logger.info("Evaluation complete: %s", metrics)


if __name__ == "__main__":
    main()
