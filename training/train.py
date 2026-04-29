"""
Liquidation Predictor — Training Script
=========================================
Trains a binary XGBoost classifier to predict the probability that a DeFi
borrow position will be liquidated within 24 hours.

Features are read from nessie.gold.ml_features via Trino. Training uses
time-series cross-validation (walk-forward) to avoid data leakage from
future market conditions.

All runs are logged to MLflow. The champion model is registered in the
MLflow Model Registry under the name "liquidation-predictor".

Usage:
    python training/train.py
    python training/train.py --n-cv-folds 5 --holdout-days 14

MLflow UI: http://localhost:5000
"""

from __future__ import annotations

import argparse
import logging
import os
import subprocess
import sys
from pathlib import Path

import mlflow
import mlflow.xgboost
import numpy as np
import pandas as pd
import trino
import xgboost as xgb
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    average_precision_score,
    brier_score_loss,
    f1_score,
    roc_auc_score,
)
from sklearn.model_selection import TimeSeriesSplit
from sklearn.preprocessing import StandardScaler

sys.path.insert(0, str(Path(__file__).parents[1]))
from features.feature_definitions import ALL_FEATURES, LABEL_COL

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
EXPERIMENT_NAME = "defi-liquidation-predictor"
MODEL_REGISTRY_NAME = "liquidation-predictor"

TRINO_HOST = os.getenv("TRINO_HOST", "localhost")
TRINO_PORT = int(os.getenv("TRINO_PORT", "8081"))

# XGBoost default params (overridden by Optuna in hyperparameter_search.py)
DEFAULT_XGB_PARAMS: dict = {
    "n_estimators": 300,
    "max_depth": 6,
    "learning_rate": 0.05,
    "subsample": 0.8,
    "colsample_bytree": 0.8,
    "min_child_weight": 5,
    "reg_alpha": 0.1,
    "reg_lambda": 1.0,
    "eval_metric": "aucpr",
    "random_state": 42,
    "n_jobs": -1,
}


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------


def load_features_from_trino() -> pd.DataFrame:
    """Read ml_features from Trino into a pandas DataFrame."""
    logger.info("Connecting to Trino at %s:%d...", TRINO_HOST, TRINO_PORT)
    conn = trino.dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user="training",
        catalog="lakehouse",
        schema="gold",
    )
    query = f"""
        SELECT
            snapshot_date,
            {', '.join(ALL_FEATURES)},
            {LABEL_COL}
        FROM ml_features
        ORDER BY snapshot_date ASC, snapshot_id ASC
    """
    logger.info("Executing feature query...")
    df = pd.read_sql(query, conn)
    logger.info("Loaded %d rows from ml_features.", len(df))
    return df


def load_features_from_parquet(path: str) -> pd.DataFrame:
    """Fallback: load features from a local parquet file (DVC pull)."""
    logger.info("Loading features from parquet: %s", path)
    df = pd.read_parquet(path)
    logger.info("Loaded %d rows from parquet.", len(df))
    return df


# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------


def compute_metrics(y_true: np.ndarray, y_prob: np.ndarray) -> dict[str, float]:
    """Compute all evaluation metrics for a set of predictions."""
    threshold = 0.3

    # Precision at top 10% predicted risk
    n_top = max(1, int(len(y_prob) * 0.10))
    top_idx = np.argsort(y_prob)[::-1][:n_top]
    precision_top10 = y_true[top_idx].mean()

    return {
        "auc_roc": roc_auc_score(y_true, y_prob),
        "average_precision": average_precision_score(y_true, y_prob),
        "brier_score": brier_score_loss(y_true, y_prob),
        "f1_at_0.3": f1_score(y_true, (y_prob >= threshold).astype(int), zero_division=0),
        "precision_top10pct": float(precision_top10),
    }


# ---------------------------------------------------------------------------
# Baseline models
# ---------------------------------------------------------------------------


def evaluate_rule_based(X: pd.DataFrame, y: pd.Series) -> dict[str, float]:
    """
    Baseline 1: rule-based (liquidated if health_factor < 1.05).
    This is the existing dbt tier system we are trying to beat.
    """
    hf = X["health_factor"].fillna(999.0)
    y_prob = np.where(hf < 1.05, 0.9, 0.1).astype(float)
    metrics = compute_metrics(y.values, y_prob)
    logger.info("Rule-based baseline: %s", metrics)
    return metrics


def train_logistic_regression(
    X_train: pd.DataFrame,
    y_train: pd.Series,
    X_val: pd.DataFrame,
    y_val: pd.Series,
) -> dict[str, float]:
    """Baseline 2: logistic regression (linear benchmark)."""
    scaler = StandardScaler()
    X_tr_scaled = scaler.fit_transform(X_train.fillna(0))
    X_v_scaled = scaler.transform(X_val.fillna(0))

    n_pos = y_train.sum()
    n_neg = len(y_train) - n_pos
    class_weight = {0: 1.0, 1: max(n_neg / max(n_pos, 1), 1.0)}

    lr = LogisticRegression(
        C=0.1,
        max_iter=1000,
        class_weight=class_weight,
        random_state=42,
    )
    lr.fit(X_tr_scaled, y_train)
    y_prob = lr.predict_proba(X_v_scaled)[:, 1]
    return compute_metrics(y_val.values, y_prob)


# ---------------------------------------------------------------------------
# Time-series cross-validation
# ---------------------------------------------------------------------------


def run_time_series_cv(
    df: pd.DataFrame,
    params: dict,
    n_splits: int = 4,
    holdout_days: int = 14,
) -> tuple[list[dict[str, float]], pd.DataFrame, pd.Series]:
    """
    Walk-forward time-series cross-validation.

    Returns:
        cv_metrics: list of metric dicts, one per fold
        X_holdout: feature DataFrame for the held-out final period
        y_holdout: label Series for the held-out final period
    """
    df = df.sort_values("snapshot_date").reset_index(drop=True)

    # Hold out the final N days as the true holdout (never used in any fold)
    max_date = pd.to_datetime(df["snapshot_date"]).max()
    holdout_cutoff = max_date - pd.Timedelta(days=holdout_days)

    train_val_df = df[pd.to_datetime(df["snapshot_date"]) <= holdout_cutoff]
    holdout_df = df[pd.to_datetime(df["snapshot_date"]) > holdout_cutoff]

    logger.info(
        "Train/val set: %d rows | Holdout: %d rows (last %d days)",
        len(train_val_df),
        len(holdout_df),
        holdout_days,
    )

    X = train_val_df[ALL_FEATURES].fillna(-999.0)
    y = train_val_df[LABEL_COL]

    tscv = TimeSeriesSplit(n_splits=n_splits)
    cv_metrics: list[dict[str, float]] = []

    for fold, (train_idx, val_idx) in enumerate(tscv.split(X), start=1):
        X_train, X_val = X.iloc[train_idx], X.iloc[val_idx]
        y_train, y_val = y.iloc[train_idx], y.iloc[val_idx]

        n_pos = int(y_train.sum())
        n_neg = len(y_train) - n_pos
        scale_pos = max(n_neg / max(n_pos, 1), 1.0)

        fold_params = {**params, "scale_pos_weight": scale_pos}
        model = xgb.XGBClassifier(**fold_params)
        model.fit(
            X_train,
            y_train,
            eval_set=[(X_val, y_val)],
            verbose=False,
        )

        y_prob = model.predict_proba(X_val)[:, 1]
        metrics = compute_metrics(y_val.values, y_prob)
        cv_metrics.append(metrics)
        logger.info("Fold %d: %s", fold, metrics)

    X_holdout = holdout_df[ALL_FEATURES].fillna(-999.0)
    y_holdout = holdout_df[LABEL_COL]

    return cv_metrics, X_holdout, y_holdout


# ---------------------------------------------------------------------------
# Main training run
# ---------------------------------------------------------------------------


def train(
    params: dict | None = None,
    n_cv_folds: int = 4,
    holdout_days: int = 14,
    data_path: str | None = None,
) -> str:
    """
    Train the liquidation predictor and log to MLflow.

    Returns the MLflow run ID of the champion model.
    """
    xgb_params = params or DEFAULT_XGB_PARAMS

    # Load data
    df = load_features_from_parquet(data_path) if data_path else load_features_from_trino()

    if len(df) == 0:
        raise ValueError("No training data loaded. Run the feature pipeline first.")

    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(EXPERIMENT_NAME)

    git_commit = _get_git_commit()

    with mlflow.start_run(run_name="xgboost-cv") as run:
        mlflow.set_tag("git_commit", git_commit)
        if data_path:
            mlflow.set_tag("data_source", data_path)
        mlflow.set_tag("training_rows", str(len(df)))
        mlflow.set_tag("label_positive_rate", f"{df[LABEL_COL].mean():.4f}")

        # Log params
        mlflow.log_params(xgb_params)
        mlflow.log_param("n_cv_folds", n_cv_folds)
        mlflow.log_param("holdout_days", holdout_days)

        # Evaluate baselines
        X_all = df[ALL_FEATURES].fillna(-999.0)
        y_all = df[LABEL_COL]

        rule_metrics = evaluate_rule_based(X_all, y_all)
        for k, v in rule_metrics.items():
            mlflow.log_metric(f"baseline_rule_{k}", v)

        # Time-series CV with XGBoost
        cv_metrics, X_holdout, y_holdout = run_time_series_cv(
            df, xgb_params, n_splits=n_cv_folds, holdout_days=holdout_days
        )

        # Log mean CV metrics
        for metric_name in cv_metrics[0]:
            mean_val = np.mean([m[metric_name] for m in cv_metrics])
            mlflow.log_metric(f"cv_mean_{metric_name}", mean_val)

        # Train final model on all train/val data (no holdout)
        holdout_cutoff = (
            pd.to_datetime(df["snapshot_date"]).max()
            - pd.Timedelta(days=holdout_days)
        )
        train_df = df[pd.to_datetime(df["snapshot_date"]) <= holdout_cutoff]
        X_train = train_df[ALL_FEATURES].fillna(-999.0)
        y_train = train_df[LABEL_COL]

        n_pos = int(y_train.sum())
        n_neg = len(y_train) - n_pos
        final_params = {**xgb_params, "scale_pos_weight": max(n_neg / max(n_pos, 1), 1.0)}

        final_model = xgb.XGBClassifier(**final_params)
        final_model.fit(X_train, y_train, verbose=False)

        # Evaluate on holdout
        y_holdout_prob = final_model.predict_proba(X_holdout)[:, 1]
        holdout_metrics = compute_metrics(y_holdout.values, y_holdout_prob)
        for k, v in holdout_metrics.items():
            mlflow.log_metric(f"holdout_{k}", v)

        logger.info("Holdout metrics: %s", holdout_metrics)

        # Log the model
        mlflow.xgboost.log_model(
            final_model,
            artifact_path="model",
            registered_model_name=MODEL_REGISTRY_NAME,
        )

        run_id = run.info.run_id
        logger.info("MLflow run ID: %s", run_id)

    return run_id


def _get_git_commit() -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"], text=True
        ).strip()
    except Exception:
        return "unknown"


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(description="Train the DeFi liquidation predictor")
    parser.add_argument("--n-cv-folds", type=int, default=4)
    parser.add_argument("--holdout-days", type=int, default=14)
    parser.add_argument("--data-path", type=str, default=None, help="Path to parquet file")
    args = parser.parse_args()

    run_id = train(
        n_cv_folds=args.n_cv_folds,
        holdout_days=args.holdout_days,
        data_path=args.data_path,
    )
    logger.info("Training complete. MLflow run: %s", run_id)
    logger.info("View results: %s/#/experiments", MLFLOW_TRACKING_URI)


if __name__ == "__main__":
    main()
