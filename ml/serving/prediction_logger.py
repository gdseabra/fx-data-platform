"""ml/serving/prediction_logger.py

Logs each inference result to the Redpanda topic `fx.predictions`.

Each message contains:
  - input features
  - output: anomaly_score, is_anomaly, risk_level
  - model version and latency
  - timestamp

These logs are consumed downstream to:
  - Compute data drift (feature distribution shift)
  - Monitor concept drift (labels vs scores over time)
  - Build a labelled dataset for future supervised retraining
"""

from __future__ import annotations

import json
import logging
import os
import threading
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)

_TOPIC = os.environ.get("PREDICTIONS_TOPIC", "fx.predictions")
_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")


class PredictionLogger:
    """Async prediction logger backed by Redpanda / Kafka.

    Falls back to a no-op if the broker is not reachable, so the
    inference service keeps running even without Redpanda.

    Usage:
        plogger = PredictionLogger()
        plogger.log(prediction_record)
    """

    def __init__(
        self,
        bootstrap_servers: str = _BOOTSTRAP_SERVERS,
        topic: str = _TOPIC,
    ) -> None:
        self._topic = topic
        self._producer: Any = None
        self._lock = threading.Lock()
        self._init_producer(bootstrap_servers)

    def _init_producer(self, bootstrap_servers: str) -> None:
        try:
            from confluent_kafka import Producer

            self._producer = Producer(
                {
                    "bootstrap.servers": bootstrap_servers,
                    "acks": "1",
                    "retries": 3,
                    "linger.ms": 5,
                    "batch.size": 16_384,
                    "compression.type": "lz4",
                }
            )
            logger.info("PredictionLogger connected to %s (topic=%s)", bootstrap_servers, self._topic)
        except Exception as exc:
            logger.warning(
                "PredictionLogger: Kafka unavailable (%s). Predictions will be logged locally only.",
                exc,
            )

    def log(
        self,
        transaction_id: str,
        user_id: str,
        currency: str,
        amount_brl: float,
        features: dict[str, Any],
        anomaly_score: float,
        is_anomaly: bool,
        risk_level: str,
        model_version: str,
        latency_ms: float,
    ) -> None:
        """Publish a prediction record asynchronously.

        Delivery errors are swallowed — prediction logging must never
        block or fail the inference response.
        """
        record = {
            "transaction_id": transaction_id,
            "user_id": user_id,
            "currency": currency,
            "amount_brl": amount_brl,
            "features": features,
            "anomaly_score": round(anomaly_score, 6),
            "is_anomaly": is_anomaly,
            "risk_level": risk_level,
            "model_version": model_version,
            "latency_ms": round(latency_ms, 3),
            "logged_at": datetime.now(tz=timezone.utc).isoformat(),
        }

        # Always write to local logger (picked up by Fluentd / CloudWatch agent)
        logger.info("prediction %s", json.dumps(record))

        if self._producer is None:
            return

        try:
            self._producer.produce(
                topic=self._topic,
                key=transaction_id.encode(),
                value=json.dumps(record).encode(),
                on_delivery=self._delivery_callback,
            )
            # Non-blocking poll to trigger delivery callbacks
            self._producer.poll(0)
        except Exception as exc:
            logger.warning("Failed to publish prediction to Kafka: %s", exc)

    @staticmethod
    def _delivery_callback(err, msg) -> None:
        if err:
            logger.warning("Prediction delivery failed: %s", err)
        else:
            logger.debug(
                "Prediction delivered to %s [%d] offset %d",
                msg.topic(), msg.partition(), msg.offset(),
            )

    def flush(self, timeout: float = 5.0) -> None:
        """Flush buffered messages before shutdown."""
        if self._producer:
            self._producer.flush(timeout=timeout)
