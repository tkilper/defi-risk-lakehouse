"""
Unit tests for the training evaluation metrics.

Tests the compute_metrics logic directly using sklearn — no trained model,
no MLflow connection, no Trino required.
"""

from __future__ import annotations

import numpy as np
import pytest

pytest.importorskip("sklearn", reason="scikit-learn not installed")

from sklearn.metrics import (
    average_precision_score,
    brier_score_loss,
    f1_score,
    roc_auc_score,
)


# ---------------------------------------------------------------------------
# Inline reimplementation of compute_metrics (mirrors training/train.py)
# Kept here so the test file is self-contained — no heavy train.py imports.
# ---------------------------------------------------------------------------


def compute_metrics(y_true: np.ndarray, y_prob: np.ndarray) -> dict[str, float]:
    threshold = 0.3
    n_top = max(1, int(len(y_prob) * 0.10))
    top_idx = np.argsort(y_prob)[::-1][:n_top]
    precision_top10 = float(y_true[top_idx].mean())
    return {
        "auc_roc": roc_auc_score(y_true, y_prob),
        "average_precision": average_precision_score(y_true, y_prob),
        "brier_score": brier_score_loss(y_true, y_prob),
        "f1_at_0.3": f1_score(y_true, (y_prob >= threshold).astype(int), zero_division=0),
        "precision_top10pct": precision_top10,
    }


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestComputeMetricsKeys:
    def test_all_expected_keys_present(self):
        y_true = np.array([0, 0, 1, 1])
        y_prob = np.array([0.1, 0.4, 0.35, 0.8])
        metrics = compute_metrics(y_true, y_prob)
        expected = {"auc_roc", "average_precision", "brier_score", "f1_at_0.3", "precision_top10pct"}
        assert set(metrics.keys()) == expected

    def test_all_values_are_floats(self):
        y_true = np.array([0, 0, 1, 1])
        y_prob = np.array([0.1, 0.4, 0.35, 0.8])
        metrics = compute_metrics(y_true, y_prob)
        for k, v in metrics.items():
            assert isinstance(v, float), f"{k} is not a float: {type(v)}"


class TestPerfectClassifier:
    def test_perfect_auc_roc(self):
        y_true = np.array([0, 0, 0, 1, 1, 1])
        y_prob = np.array([0.0, 0.0, 0.0, 1.0, 1.0, 1.0])
        metrics = compute_metrics(y_true, y_prob)
        assert metrics["auc_roc"] == pytest.approx(1.0)

    def test_perfect_average_precision(self):
        y_true = np.array([0, 0, 0, 1, 1, 1])
        y_prob = np.array([0.0, 0.0, 0.0, 1.0, 1.0, 1.0])
        metrics = compute_metrics(y_true, y_prob)
        assert metrics["average_precision"] == pytest.approx(1.0)

    def test_perfect_brier_score_is_zero(self):
        y_true = np.array([0, 0, 1, 1])
        y_prob = np.array([0.0, 0.0, 1.0, 1.0])
        metrics = compute_metrics(y_true, y_prob)
        assert metrics["brier_score"] == pytest.approx(0.0)


class TestWorstClassifier:
    def test_inverse_classifier_has_low_auc(self):
        """A classifier that predicts the opposite of truth should have AUC near 0."""
        y_true = np.array([0, 0, 0, 1, 1, 1])
        y_prob = np.array([1.0, 1.0, 1.0, 0.0, 0.0, 0.0])  # perfectly wrong
        metrics = compute_metrics(y_true, y_prob)
        assert metrics["auc_roc"] == pytest.approx(0.0)


class TestBrierScore:
    def test_brier_penalises_confident_wrong_predictions(self):
        """High-confidence wrong predictions should yield a worse Brier score."""
        y_true = np.array([0, 0, 1, 1])

        # Mildly wrong
        mild = compute_metrics(y_true, np.array([0.4, 0.4, 0.6, 0.6]))
        # Confidently wrong
        confident = compute_metrics(y_true, np.array([0.9, 0.9, 0.1, 0.1]))

        assert confident["brier_score"] > mild["brier_score"]

    def test_brier_score_is_between_0_and_1(self):
        y_true = np.array([0, 1, 0, 1])
        y_prob = np.array([0.3, 0.7, 0.2, 0.8])
        metrics = compute_metrics(y_true, y_prob)
        assert 0.0 <= metrics["brier_score"] <= 1.0


class TestPrecisionTop10Pct:
    def test_top_10pct_selects_highest_risk_positions(self):
        """Top 10% should select the positions with the highest predicted probability."""
        n = 100
        y_true = np.zeros(n)
        y_prob = np.random.default_rng(42).uniform(0, 1, n)

        # Mark the top 10 as true positives
        top_10_idx = np.argsort(y_prob)[::-1][:10]
        y_true[top_10_idx] = 1

        metrics = compute_metrics(y_true, y_prob)
        # Perfect precision: all flagged positions are true positives
        assert metrics["precision_top10pct"] == pytest.approx(1.0)

    def test_top_10pct_at_least_one_sample(self):
        """With fewer than 10 samples, n_top should be at least 1 (not 0)."""
        y_true = np.array([1])
        y_prob = np.array([0.9])
        metrics = compute_metrics(y_true, y_prob)
        # Should not raise and should return 1.0 precision
        assert metrics["precision_top10pct"] == pytest.approx(1.0)


class TestF1AtThreshold:
    def test_f1_is_zero_when_no_predictions_above_threshold(self):
        y_true = np.array([0, 0, 1, 1])
        y_prob = np.array([0.1, 0.1, 0.1, 0.1])  # all below 0.3 threshold
        metrics = compute_metrics(y_true, y_prob)
        assert metrics["f1_at_0.3"] == pytest.approx(0.0)

    def test_f1_high_when_threshold_captures_positives(self):
        y_true = np.array([0, 0, 1, 1])
        y_prob = np.array([0.1, 0.1, 0.9, 0.9])  # perfect separation at 0.3
        metrics = compute_metrics(y_true, y_prob)
        assert metrics["f1_at_0.3"] == pytest.approx(1.0)
