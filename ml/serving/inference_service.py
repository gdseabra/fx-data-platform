"""ml/serving/inference_service.py

FastAPI real-time inference service for FX transaction anomaly detection.

Endpoints:
  POST /predict   — score a single transaction
  GET  /health    — liveness + readiness check
  GET  /metrics   — Prometheus exposition format
  GET  /          — service info

Feature retrieval:
  - User features fetched from Feast online store (user_transaction_features,
    transaction_pattern_features).
  - On-demand features (z_score, velocity) computed locally.

Latency target: p95 < 100ms.

Run locally:
    uvicorn ml.serving.inference_service:app --host 0.0.0.0 --port 8000 --workers 2
"""

from __future__ import annotations

import logging
import os
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import structlog
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import PlainTextResponse
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)
from pydantic import BaseModel, Field

from ml.serving.model_loader import ModelLoader
from ml.serving.prediction_logger import PredictionLogger

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
structlog.configure(
    processors=[
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer(),
    ],
    logger_factory=structlog.stdlib.LoggerFactory(),
)
logger = structlog.get_logger(__name__)
std_logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Prometheus metrics
# ---------------------------------------------------------------------------
REQUEST_COUNT = Counter(
    "inference_requests_total",
    "Total prediction requests",
    ["status", "risk_level"],
)
REQUEST_LATENCY = Histogram(
    "inference_latency_ms",
    "Prediction latency in milliseconds",
    buckets=[5, 10, 25, 50, 75, 100, 200, 500],
)
ANOMALY_SCORE = Histogram(
    "inference_anomaly_score",
    "Distribution of anomaly scores",
    buckets=[0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0],
)
MODEL_VERSION_GAUGE = Gauge(
    "inference_model_version_info",
    "Current serving model version",
    ["version"],
)
FEAST_FEATURE_ERRORS = Counter(
    "feast_feature_fetch_errors_total",
    "Number of times Feast feature fetch failed (fallback to zeros)",
)

# ---------------------------------------------------------------------------
# Global singletons (created on startup via lifespan)
# ---------------------------------------------------------------------------
_model_loader: ModelLoader | None = None
_prediction_logger: PredictionLogger | None = None
_feast_store: Any = None  # feast.FeatureStore or None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _model_loader, _prediction_logger, _feast_store

    # Model loader
    _model_loader = ModelLoader()
    _model_loader.start()

    # Prediction logger
    _prediction_logger = PredictionLogger()

    # Feast (optional — graceful degradation if not available)
    feast_repo = os.environ.get("FEAST_REPO_PATH", "ml/feature_store/feature_repo")
    try:
        from feast import FeatureStore
        _feast_store = FeatureStore(repo_path=feast_repo)
        logger.info("Feast online store connected", repo_path=feast_repo)
    except Exception as exc:
        logger.warning("Feast unavailable — predictions will use request-only features", error=str(exc))

    logger.info("Inference service started")
    yield

    # Shutdown
    _model_loader.stop()
    if _prediction_logger:
        _prediction_logger.flush()
    logger.info("Inference service stopped")


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------
app = FastAPI(
    title="FX Anomaly Detection Service",
    version="1.0.0",
    description="Real-time anomaly scoring for FX transactions. p95 latency < 100ms.",
    lifespan=lifespan,
)


# ---------------------------------------------------------------------------
# Request / Response models
# ---------------------------------------------------------------------------

class TransactionRequest(BaseModel):
    transaction_id: str = Field(..., description="Unique transaction UUID")
    user_id: str = Field(..., description="User UUID")
    amount_brl: float = Field(..., ge=0, description="Transaction amount in BRL")
    currency: str = Field(..., pattern="^[A-Z]{3}$", description="ISO-4217 currency code")
    timestamp: datetime = Field(default_factory=lambda: datetime.now(tz=timezone.utc))
    spread_pct: Optional[float] = Field(None, ge=0)


class AnomalyResponse(BaseModel):
    transaction_id: str
    anomaly_score: float = Field(..., ge=0, le=1, description="0=normal, 1=highly anomalous")
    is_anomaly: bool
    risk_level: str = Field(..., description="low | medium | high")
    feature_contributions: Dict[str, float]
    model_version: str
    latency_ms: float


class HealthResponse(BaseModel):
    status: str
    model_loaded: bool
    model_version: str
    feast_available: bool
    last_model_update: Optional[datetime]
    dependencies: Dict[str, bool]


# ---------------------------------------------------------------------------
# Feature retrieval
# ---------------------------------------------------------------------------

def _fetch_feast_features(user_id: str, currency: str) -> dict[str, float]:
    """Fetch user + currency features from Feast online store.

    Returns empty dict on failure so inference can proceed with defaults.
    """
    if _feast_store is None:
        return {}
    try:
        import pandas as pd

        user_features = _feast_store.get_online_features(
            features=[
                "user_transaction_features:total_transactions_30d",
                "user_transaction_features:total_volume_30d",
                "user_transaction_features:avg_ticket_30d",
                "user_transaction_features:days_since_registration",
                "user_transaction_features:tier",
                "transaction_pattern_features:transactions_last_1h",
                "transaction_pattern_features:avg_amount_last_24h",
                "transaction_pattern_features:night_transaction_ratio",
            ],
            entity_rows=[{"user_id": user_id}],
        ).to_dict()

        currency_features = _feast_store.get_online_features(
            features=[
                "currency_features:current_rate",
                "currency_features:volatility_7d",
            ],
            entity_rows=[{"currency": currency}],
        ).to_dict()

        result: dict[str, float] = {}
        for k, v in {**user_features, **currency_features}.items():
            val = v[0] if isinstance(v, list) else v
            if val is not None:
                try:
                    result[k] = float(val)
                except (TypeError, ValueError):
                    pass
        return result

    except Exception as exc:
        FEAST_FEATURE_ERRORS.inc()
        std_logger.warning("Feast feature fetch failed for user=%s: %s", user_id, exc)
        return {}


def _build_feature_vector(request: TransactionRequest, feast_features: dict) -> dict[str, float]:
    """Merge request data with Feast features into a flat feature dict."""
    hour = request.timestamp.hour
    dow = request.timestamp.weekday() + 1  # 1=Monday
    is_business = 1.0 if (8 <= hour <= 18 and dow <= 5) else 0.0

    avg_ticket = feast_features.get("avg_ticket_30d", request.amount_brl)
    avg_24h = feast_features.get("avg_amount_last_24h", request.amount_brl)
    std_proxy = max(avg_24h * 0.3, 1.0)  # crude std estimate
    z_score = (request.amount_brl - avg_ticket) / std_proxy

    velocity_1h = feast_features.get("transactions_last_1h", 0.0)
    normal_velocity = max(feast_features.get("total_transactions_30d", 1.0) / 30.0, 0.1)
    velocity_score = velocity_1h / normal_velocity

    return {
        "amount_brl": request.amount_brl,
        "hour_of_day": float(hour),
        "day_of_week": float(dow),
        "is_business_hours": is_business,
        "spread_pct": request.spread_pct or 0.01,
        "velocity_1h": velocity_1h,
        "z_score_amount": float(z_score),
    }


def _risk_level(score: float) -> tuple[str, bool]:
    if score >= 0.7:
        return "high", True
    if score >= 0.4:
        return "medium", True
    return "low", False


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@app.post("/predict", response_model=AnomalyResponse)
async def predict(request: TransactionRequest) -> AnomalyResponse:
    """Score a single FX transaction for anomaly risk."""
    if _model_loader is None or not _model_loader.is_ready:
        raise HTTPException(status_code=503, detail="Model not loaded")

    t_start = time.perf_counter()

    # 1. Features
    feast_features = _fetch_feast_features(request.user_id, request.currency)
    feature_vector = _build_feature_vector(request, feast_features)

    # 2. Inference
    import numpy as np
    import pandas as pd

    model = _model_loader.model
    X = pd.DataFrame([feature_vector])

    try:
        raw = model.predict(X)
        # pyfunc.predict returns array-like; IsolationForest returns +1/-1
        pred_val = float(raw[0]) if hasattr(raw, "__len__") else float(raw)
    except Exception as exc:
        logger.error("Model predict failed", error=str(exc))
        raise HTTPException(status_code=500, detail=f"Inference error: {exc}")

    # Normalise IsolationForest output (-1/+1) → anomaly score 0-1
    # score_samples() returns negative values; higher (less negative) = more normal
    try:
        raw_score = model._model_impl.score_samples(X.values)
        score_norm = float(np.clip(
            (-raw_score[0] - 0.0) / 0.5,  # rough normalisation around 0 midpoint
            0.0, 1.0,
        ))
    except Exception:
        # fallback: use the +1/-1 output directly
        score_norm = 0.8 if pred_val == -1 else 0.2

    risk, is_anomaly = _risk_level(score_norm)
    latency_ms = (time.perf_counter() - t_start) * 1000

    # 3. Prometheus
    REQUEST_COUNT.labels(status="ok", risk_level=risk).inc()
    REQUEST_LATENCY.observe(latency_ms)
    ANOMALY_SCORE.observe(score_norm)

    # 4. Prediction log
    if _prediction_logger:
        _prediction_logger.log(
            transaction_id=request.transaction_id,
            user_id=request.user_id,
            currency=request.currency,
            amount_brl=request.amount_brl,
            features=feature_vector,
            anomaly_score=score_norm,
            is_anomaly=is_anomaly,
            risk_level=risk,
            model_version=_model_loader.version,
            latency_ms=latency_ms,
        )

    return AnomalyResponse(
        transaction_id=request.transaction_id,
        anomaly_score=round(score_norm, 6),
        is_anomaly=is_anomaly,
        risk_level=risk,
        feature_contributions={k: round(v, 4) for k, v in feature_vector.items()},
        model_version=_model_loader.version,
        latency_ms=round(latency_ms, 3),
    )


@app.get("/health", response_model=HealthResponse)
async def health() -> HealthResponse:
    """Liveness + readiness check."""
    model_ok = _model_loader is not None and _model_loader.is_ready
    return HealthResponse(
        status="healthy" if model_ok else "degraded",
        model_loaded=model_ok,
        model_version=_model_loader.version if _model_loader else "none",
        feast_available=_feast_store is not None,
        last_model_update=_model_loader.loaded_at if _model_loader else None,
        dependencies={
            "model": model_ok,
            "feast": _feast_store is not None,
        },
    )


@app.get("/metrics", response_class=PlainTextResponse)
async def metrics() -> PlainTextResponse:
    """Prometheus metrics endpoint."""
    if _model_loader and _model_loader.is_ready:
        MODEL_VERSION_GAUGE.labels(version=_model_loader.version).set(1)
    data = generate_latest()
    return PlainTextResponse(content=data.decode(), media_type=CONTENT_TYPE_LATEST)


@app.get("/")
async def root() -> dict:
    return {
        "service": "FX Anomaly Detection Service",
        "version": "1.0.0",
        "endpoints": {
            "POST /predict": "Score a transaction",
            "GET /health": "Health check",
            "GET /metrics": "Prometheus metrics",
            "GET /docs": "OpenAPI docs",
        },
    }


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "ml.serving.inference_service:app",
        host="0.0.0.0",
        port=int(os.environ.get("PORT", "8000")),
        workers=int(os.environ.get("WORKERS", "2")),
        log_level="info",
    )
