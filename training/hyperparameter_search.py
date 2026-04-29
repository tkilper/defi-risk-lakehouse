"""
Hyperparameter Search with Optuna
===================================
Runs 50 Optuna trials to find the best XGBoost hyperparameters, with each
trial logged as a nested MLflow run under the parent experiment.

The best parameters are used to train the final champion model.

Usage:
    python training/hyperparameter_search.py
    python training/hyperparameter_search.py --n-trials 50
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import mlflow
import numpy as np
import optuna
import pandas as pd

sys.path.insert(0, str(Path(__file__).parents[1]))
from training.train import (
    EXPERIMENT_NAME,
    MLFLOW_TRACKING_URI,
    load_features_from_trino,
    run_time_series_cv,
    train,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

# Silence Optuna's verbose trial logging
optuna.logging.set_verbosity(optuna.logging.WARNING)


def make_objective(df: pd.DataFrame, parent_run_id: str, n_cv_folds: int = 4):
    """Return an Optuna objective function that logs each trial to MLflow."""

    def objective(trial: optuna.Trial) -> float:
        params = {
            "n_estimators": trial.suggest_int("n_estimators", 100, 600),
            "max_depth": trial.suggest_int("max_depth", 3, 8),
            "learning_rate": trial.suggest_float("learning_rate", 0.01, 0.2, log=True),
            "subsample": trial.suggest_float("subsample", 0.6, 1.0),
            "colsample_bytree": trial.suggest_float("colsample_bytree", 0.5, 1.0),
            "min_child_weight": trial.suggest_int("min_child_weight", 1, 20),
            "reg_alpha": trial.suggest_float("reg_alpha", 1e-4, 10.0, log=True),
            "reg_lambda": trial.suggest_float("reg_lambda", 1e-4, 10.0, log=True),
            "eval_metric": "aucpr",
            "random_state": 42,
            "n_jobs": -1,
        }

        mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
        mlflow.set_experiment(EXPERIMENT_NAME)

        with mlflow.start_run(
            run_name=f"trial-{trial.number}",
            nested=True,
            tags={"parent_run_id": parent_run_id, "optuna_trial": str(trial.number)},
        ):
            mlflow.log_params(params)

            cv_metrics, _, _ = run_time_series_cv(df, params, n_splits=n_cv_folds)
            mean_ap = np.mean([m["average_precision"] for m in cv_metrics])
            mean_auc = np.mean([m["auc_roc"] for m in cv_metrics])

            mlflow.log_metric("cv_mean_average_precision", mean_ap)
            mlflow.log_metric("cv_mean_auc_roc", mean_auc)

        return mean_ap  # maximize average precision

    return objective


def run_search(n_trials: int = 50, n_cv_folds: int = 4) -> dict:
    """
    Run the Optuna hyperparameter search.

    Returns the best parameters found.
    """
    df = load_features_from_trino()
    if len(df) == 0:
        raise ValueError("No training data. Run the feature pipeline first.")

    logger.info("Starting hyperparameter search: %d trials, %d CV folds", n_trials, n_cv_folds)

    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(EXPERIMENT_NAME)

    with mlflow.start_run(run_name="optuna-search") as parent_run:
        parent_run_id = parent_run.info.run_id
        mlflow.log_param("n_trials", n_trials)
        mlflow.log_param("n_cv_folds", n_cv_folds)
        mlflow.log_param("objective", "maximize average_precision")

        study = optuna.create_study(direction="maximize")
        study.optimize(
            make_objective(df, parent_run_id, n_cv_folds),
            n_trials=n_trials,
            show_progress_bar=True,
        )

        best_params = study.best_params
        best_value = study.best_value

        logger.info("Best average precision: %.4f", best_value)
        logger.info("Best params: %s", best_params)

        mlflow.log_metric("best_cv_average_precision", best_value)
        mlflow.log_params({f"best_{k}": v for k, v in best_params.items()})

    # Train final champion model with the best params
    logger.info("Training final champion model with best params...")
    full_params = {
        **best_params,
        "eval_metric": "aucpr",
        "random_state": 42,
        "n_jobs": -1,
    }
    champion_run_id = train(params=full_params, n_cv_folds=n_cv_folds)
    logger.info("Champion model MLflow run: %s", champion_run_id)

    return best_params


def main() -> None:
    parser = argparse.ArgumentParser(description="Optuna hyperparameter search")
    parser.add_argument("--n-trials", type=int, default=50)
    parser.add_argument("--n-cv-folds", type=int, default=4)
    args = parser.parse_args()

    best = run_search(n_trials=args.n_trials, n_cv_folds=args.n_cv_folds)
    logger.info("Search complete. Best params: %s", best)


if __name__ == "__main__":
    main()
