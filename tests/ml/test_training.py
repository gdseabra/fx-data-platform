# tests/ml/test_training.py
# Unit tests for the Phase 4 ML training pipeline.

import numpy as np
import pandas as pd
import pytest

from ml.training.train_anomaly_detector import (
    evaluate,
    prepare_features,
    temporal_split,
    train_isolation_forest,
    train_lof,
)


def _sample_df(n: int = 200, anomaly_rate: float = 0.05) -> pd.DataFrame:
    rng = np.random.default_rng(42)
    n_anom = int(n * anomaly_rate)
    return pd.DataFrame(
        {
            "amount_brl": rng.exponential(2_000, n),
            "hour_of_day": rng.integers(0, 24, n),
            "day_of_week": rng.integers(1, 8, n),
            "is_business_hours": rng.choice([True, False], n),
            "spread_pct": rng.uniform(0.005, 0.03, n),
            "velocity_1h": rng.integers(0, 10, n),
            "z_score_amount": rng.normal(0, 1, n),
            "is_anomaly": np.concatenate([np.zeros(n - n_anom), np.ones(n_anom)]),
        }
    )


@pytest.mark.unit
def test_prepare_features_shape():
    df = _sample_df(100)
    X, y, scaler = prepare_features(df)
    assert X.shape == (100, 7), f"expected (100, 7), got {X.shape}"
    assert y.shape == (100,)
    assert scaler is not None


@pytest.mark.unit
def test_prepare_features_scaled():
    df = _sample_df(500)
    X, _, _ = prepare_features(df)
    # StandardScaler → columns should be approximately mean=0, std=1
    assert abs(X.mean()) < 0.5
    assert abs(X.std() - 1.0) < 0.5


@pytest.mark.unit
def test_temporal_split_sizes():
    df = _sample_df(1000)
    X, y, _ = prepare_features(df)
    X_tr, y_tr, X_val, y_val, X_te, y_te = temporal_split(X, y)
    assert len(X_tr) == 700
    assert len(X_val) == 150
    assert len(X_te) == 150
    assert len(X_tr) + len(X_val) + len(X_te) == 1000


@pytest.mark.unit
def test_temporal_split_no_shuffle():
    """The temporal split must preserve order (first 70% → train, etc.)."""
    df = _sample_df(100)
    X, y, _ = prepare_features(df)
    X_tr, _, _, _, X_te, _ = temporal_split(X, y)
    # Training set ends where test set begins — no overlap
    assert not np.array_equal(X_tr[-1], X_te[0])


@pytest.mark.unit
def test_train_isolation_forest():
    df = _sample_df(300)
    X, _, _ = prepare_features(df)
    model = train_isolation_forest(X, hp={"contamination": 0.05, "n_estimators": 10, "random_state": 42})
    preds = model.predict(X)
    assert set(preds).issubset({-1, 1}), "IsolationForest must output +1 or -1"


@pytest.mark.unit
def test_train_lof():
    df = _sample_df(300)
    X, _, _ = prepare_features(df)
    model = train_lof(X, hp={"contamination": 0.05, "n_neighbors": 5})
    preds = model.predict(X)
    assert set(preds).issubset({-1, 1})


@pytest.mark.unit
def test_evaluate_returns_expected_keys():
    df = _sample_df(300)
    X, y, _ = prepare_features(df)
    model = train_isolation_forest(X, hp={"contamination": 0.05, "n_estimators": 10, "random_state": 42})
    metrics = evaluate(model, X, y, n_latency_samples=20)
    expected_keys = {"precision", "recall", "f1", "auc_roc", "latency_p50_ms", "latency_p95_ms", "latency_p99_ms"}
    assert expected_keys == set(metrics.keys())


@pytest.mark.unit
def test_evaluate_metric_ranges():
    df = _sample_df(300)
    X, y, _ = prepare_features(df)
    model = train_isolation_forest(X, hp={"contamination": 0.05, "n_estimators": 10, "random_state": 42})
    metrics = evaluate(model, X, y, n_latency_samples=20)
    for key in ("precision", "recall", "f1", "auc_roc"):
        assert 0.0 <= metrics[key] <= 1.0, f"{key}={metrics[key]} out of [0,1]"
    assert metrics["latency_p95_ms"] >= 0
