"""ml/training/train_anomaly_detector.py

Full anomaly-detection training pipeline for FX transactions.

Trains two unsupervised models:
  - Isolation Forest (primary)
  - Local Outlier Factor  (comparison)

Features are fetched from the Feast offline store (or a local Parquet
fallback when Feast is not available), split *temporally* (no random
shuffle), and all results are tracked in MLflow.

Usage:
    python -m ml.training.train_anomaly_detector \\
        --config ml/config/training_config.yaml \\
        --env dev
"""

from __future__ import annotations

import argparse
import io
import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import matplotlib

matplotlib.use("Agg")  # non-interactive backend
import matplotlib.pyplot as plt
import mlflow
import mlflow.sklearn
import numpy as np
import pandas as pd
import yaml
from sklearn.ensemble import IsolationForest
from sklearn.metrics import (
    ConfusionMatrixDisplay,
    PrecisionRecallDisplay,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.neighbors import LocalOutlierFactor
from sklearn.preprocessing import StandardScaler

logger = logging.getLogger(__name__)

_FEATURE_COLS = [
    "amount_brl",
    "hour_of_day",
    "day_of_week",
    "is_business_hours",
    "spread_pct",
    "velocity_1h",
    "z_score_amount",
]


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def _load_from_feast(config: dict, lookback_days: int) -> pd.DataFrame:
    """Fetch historical features from the Feast offline store."""
    from feast import FeatureStore

    repo_path = config.get("feast", {}).get("repo_path", "ml/feature_store/feature_repo")
    store = FeatureStore(repo_path=repo_path)

    end_date = datetime.now(tz=timezone.utc)

    # Build entity DataFrame over the time range
    entity_df = pd.DataFrame(
        {
            "user_id": ["*"],  # wildcard — replaced by actual user list in prod
            "event_timestamp": [end_date],
        }
    )

    features = [
        "user_transaction_features:total_transactions_30d",
        "user_transaction_features:total_volume_30d",
        "user_transaction_features:avg_ticket_30d",
        "transaction_pattern_features:transactions_last_1h",
        "transaction_pattern_features:avg_amount_last_24h",
        "transaction_pattern_features:night_transaction_ratio",
    ]

    df = store.get_historical_features(
        entity_df=entity_df, features=features
    ).to_df()
    logger.info("Fetched %d rows from Feast offline store", len(df))
    return df


def load_training_data(config: dict) -> pd.DataFrame:
    """Load data for training. Tries Feast first, falls back to local Parquet."""
    lookback = config.get("model_training", {}).get(
        "anomaly_detection", {}
    ).get("training_config", {}).get("lookback_days", 90)

    if config.get("feast", {}).get("enabled", False):
        try:
            return _load_from_feast(config, lookback)
        except Exception as exc:
            logger.warning("Feast unavailable (%s). Falling back to local data.", exc)

    # Local fallback — expects data/training_data.parquet
    fallback_path = os.environ.get("TRAINING_DATA_PATH", "data/training_data.parquet")
    if not Path(fallback_path).exists():
        raise FileNotFoundError(
            f"Training data not found at {fallback_path}. "
            "Run `make seed` or set TRAINING_DATA_PATH."
        )
    df = pd.read_parquet(fallback_path)
    logger.info("Loaded %d rows from local fallback: %s", len(df), fallback_path)
    return df


# ---------------------------------------------------------------------------
# Feature preparation
# ---------------------------------------------------------------------------

def prepare_features(
    df: pd.DataFrame,
    feature_cols: list[str] | None = None,
) -> tuple[np.ndarray, np.ndarray, StandardScaler]:
    """Scale and split features from a DataFrame.

    Returns:
        (X_scaled, y, scaler) where y is the is_anomaly ground-truth label.

    """
    cols = feature_cols or _FEATURE_COLS
    # Keep only columns that actually exist
    available = [c for c in cols if c in df.columns]
    missing = set(cols) - set(available)
    if missing:
        logger.warning("Missing feature columns (will be zeroed): %s", missing)
        for c in missing:
            df[c] = 0.0

    X = df[cols].fillna(0).astype(float).values
    y = df["is_anomaly"].fillna(0).astype(int).values

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    logger.info("Features shape: %s  |  anomaly rate: %.2f%%", X_scaled.shape, y.mean() * 100)
    return X_scaled, y, scaler


def temporal_split(
    X: np.ndarray,
    y: np.ndarray,
    train_ratio: float = 0.70,
    val_ratio: float = 0.15,
) -> tuple:
    """Chronological split — no shuffling for time-series integrity."""
    n = len(X)
    n_train = int(n * train_ratio)
    n_val = int(n * val_ratio)

    return (
        X[:n_train], y[:n_train],
        X[n_train: n_train + n_val], y[n_train: n_train + n_val],
        X[n_train + n_val:], y[n_train + n_val:],
    )


# ---------------------------------------------------------------------------
# Model training
# ---------------------------------------------------------------------------

def train_isolation_forest(
    X_train: np.ndarray, hp: dict
) -> IsolationForest:
    model = IsolationForest(
        contamination=hp.get("contamination", 0.05),
        n_estimators=hp.get("n_estimators", 100),
        max_samples=hp.get("max_samples", "auto"),
        random_state=hp.get("random_state", 42),
        n_jobs=-1,
    )
    model.fit(X_train)
    logger.info("IsolationForest trained.")
    return model


def train_lof(X_train: np.ndarray, hp: dict) -> LocalOutlierFactor:
    model = LocalOutlierFactor(
        n_neighbors=hp.get("n_neighbors", 20),
        contamination=hp.get("contamination", 0.05),
        novelty=True,  # enable predict() for new data
        n_jobs=-1,
    )
    model.fit(X_train)
    logger.info("LocalOutlierFactor trained.")
    return model


# ---------------------------------------------------------------------------
# Evaluation
# ---------------------------------------------------------------------------

def evaluate(
    model,
    X_test: np.ndarray,
    y_test: np.ndarray,
    n_latency_samples: int = 200,
) -> dict[str, float]:
    """Return metrics dict including latency percentiles."""
    preds = model.predict(X_test)
    binary = (preds == -1).astype(int)
    scores = -model.score_samples(X_test)  # higher → more anomalous
    scores_norm = (scores - scores.min()) / (scores.max() - scores.min() + 1e-9)

    # Latency benchmark
    sample = X_test[:n_latency_samples]
    latencies = []
    for row in sample:
        t0 = time.perf_counter()
        model.predict([row])
        latencies.append((time.perf_counter() - t0) * 1000)  # ms

    return {
        "precision": precision_score(y_test, binary, zero_division=0),
        "recall": recall_score(y_test, binary, zero_division=0),
        "f1": f1_score(y_test, binary, zero_division=0),
        "auc_roc": roc_auc_score(y_test, scores_norm) if len(np.unique(y_test)) > 1 else 0.0,
        "latency_p50_ms": float(np.percentile(latencies, 50)),
        "latency_p95_ms": float(np.percentile(latencies, 95)),
        "latency_p99_ms": float(np.percentile(latencies, 99)),
    }


# ---------------------------------------------------------------------------
# MLflow artifact helpers
# ---------------------------------------------------------------------------

def _fig_to_array(fig) -> np.ndarray:
    """Convert a matplotlib figure to an RGBA numpy array for mlflow.log_image."""
    buf = io.BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight")
    buf.seek(0)
    import PIL.Image
    return np.array(PIL.Image.open(buf))


def _log_confusion_matrix(model, X_test, y_test, label: str) -> None:
    preds = (model.predict(X_test) == -1).astype(int)
    fig, ax = plt.subplots(figsize=(5, 4))
    ConfusionMatrixDisplay.from_predictions(y_test, preds, ax=ax)
    ax.set_title(f"Confusion Matrix — {label}")
    mlflow.log_image(_fig_to_array(fig), f"confusion_matrix_{label.lower().replace(' ', '_')}.png")
    plt.close(fig)


def _log_pr_curve(model, X_test, y_test, label: str) -> None:
    scores = -model.score_samples(X_test)
    scores_norm = (scores - scores.min()) / (scores.max() - scores.min() + 1e-9)
    fig, ax = plt.subplots(figsize=(6, 5))
    PrecisionRecallDisplay.from_predictions(y_test, scores_norm, ax=ax, name=label)
    ax.set_title(f"Precision-Recall Curve — {label}")
    mlflow.log_image(_fig_to_array(fig), f"pr_curve_{label.lower().replace(' ', '_')}.png")
    plt.close(fig)


def _log_score_distribution(model, X_test, label: str) -> None:
    scores = -model.score_samples(X_test)
    fig, ax = plt.subplots(figsize=(6, 4))
    ax.hist(scores, bins=50, edgecolor="black", alpha=0.7)
    ax.set_xlabel("Anomaly Score")
    ax.set_ylabel("Count")
    ax.set_title(f"Score Distribution — {label}")
    mlflow.log_image(_fig_to_array(fig), f"score_dist_{label.lower().replace(' ', '_')}.png")
    plt.close(fig)


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def run_pipeline(config: dict, env: str = "dev") -> dict[str, Any]:
    """Execute full training pipeline for both models.

    Returns:
        Dict with run_ids, metrics, and status for both models.

    """
    mlflow_cfg = config.get("mlflow", {})
    tracking_uri = os.environ.get("MLFLOW_TRACKING_URI", mlflow_cfg.get("tracking_uri", "http://mlflow:5000"))
    mlflow.set_tracking_uri(tracking_uri)

    experiment_name = config["experiments"]["anomaly_detection"]["name"]
    mlflow.set_experiment(experiment_name)

    hp = config["model_training"]["anomaly_detection"]["hyperparameters"]
    train_cfg = config["model_training"]["anomaly_detection"]["training_config"]
    feature_list = config["model_training"]["anomaly_detection"].get("features", _FEATURE_COLS)

    df = load_training_data(config)
    X, y, scaler = prepare_features(df, feature_list)
    X_train, y_train, X_val, y_val, X_test, y_test = temporal_split(
        X, y, train_cfg["train_ratio"], train_cfg["validation_ratio"]
    )

    results = {}

    for model_type, trainer in [
        ("isolation_forest", train_isolation_forest),
        ("lof", train_lof),
    ]:
        logger.info("=== Training %s ===", model_type)
        with mlflow.start_run(run_name=f"{model_type}_{env}") as run:
            # Parameters
            mlflow.log_params({**hp, "model_type": model_type, "env": env})
            mlflow.log_params({
                "train_size": len(X_train),
                "val_size": len(X_val),
                "test_size": len(X_test),
                "features": json.dumps(feature_list),
                "lookback_days": train_cfg["lookback_days"],
                "anomaly_rate_pct": round(float(y.mean()) * 100, 2),
            })
            mlflow.set_tags({
                "model_type": model_type,
                "data_version": datetime.now(tz=timezone.utc).strftime("%Y%m%d"),
                "environment": env,
            })

            # Train
            model = trainer(X_train, hp)

            # Evaluate on test set
            metrics = evaluate(model, X_test, y_test)
            mlflow.log_metrics(metrics)
            logger.info("%s metrics: %s", model_type, metrics)

            # Artifacts
            mlflow.sklearn.log_model(model, artifact_path=model_type)
            _log_confusion_matrix(model, X_test, y_test, model_type)
            _log_pr_curve(model, X_test, y_test, model_type)
            _log_score_distribution(model, X_test, model_type)

            results[model_type] = {
                "run_id": run.info.run_id,
                "metrics": metrics,
                "status": "success",
            }

    return results


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Train FX anomaly detector")
    parser.add_argument("--config", default="ml/config/training_config.yaml")
    parser.add_argument("--env", default="dev", choices=["dev", "staging", "prod"])
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(level=args.log_level, format="%(asctime)s %(levelname)s %(message)s")

    with open(args.config) as f:
        config = yaml.safe_load(f)

    results = run_pipeline(config, env=args.env)
    print(json.dumps(results, indent=2, default=str))


if __name__ == "__main__":
    main()
