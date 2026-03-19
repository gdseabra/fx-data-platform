# FX Data Platform - Exchange Rate Producer
# Publishes live FX exchange rates to Redpanda using a random walk simulation.
# Flags anomalies when the rate change exceeds 2 standard deviations.

import json
import signal
import time
from argparse import ArgumentParser
from collections import deque
from datetime import datetime, timezone
from statistics import mean, stdev
from typing import Any, Dict

import structlog
from confluent_kafka import Producer

from ingestion.producer.config import ProducerConfig

structlog.configure(
    processors=[
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer(),
    ],
    logger_factory=structlog.stdlib.LoggerFactory(),
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger(__name__)

# Base rates relative to BRL (approximate mid-market)
_BASE_RATES: Dict[str, float] = {
    "USD": 5.0,
    "EUR": 5.5,
    "GBP": 6.3,
    "JPY": 0.034,
    "AUD": 3.3,
    "CAD": 3.7,
}

# Rolling window for anomaly detection (last N observations per pair)
_WINDOW_SIZE = 20
_ANOMALY_STD_THRESHOLD = 2.0


class ExchangeRateProducer:
    """Publishes simulated FX exchange rates to Redpanda every N seconds.

    Uses a random walk to simulate realistic rate movements and flags
    anomalies when the percentage change exceeds 2 standard deviations
    of the recent history.

    Usage::

        producer = ExchangeRateProducer()
        producer.run(interval_seconds=5, duration_seconds=300)

    CLI::

        python -m ingestion.producer.exchange_rate_producer --interval 5 --duration 300
    """

    def __init__(self, config: ProducerConfig | None = None):
        self._config = config or ProducerConfig()
        self._rates: Dict[str, float] = dict(_BASE_RATES)
        # Per-pair rolling window of recent pct changes for std dev calculation
        self._change_history: Dict[str, deque] = {
            pair: deque(maxlen=_WINDOW_SIZE) for pair in _BASE_RATES
        }
        self._running = True
        self._messages_sent = 0
        self._messages_failed = 0

        self._producer = Producer(self._config.get_producer_config())

        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)

        logger.info("exchange_rate_producer_initialized",
                    bootstrap_servers=self._config.bootstrap_servers,
                    currencies=list(_BASE_RATES.keys()))

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _handle_shutdown(self, sig, frame) -> None:
        logger.info("shutdown_signal_received", signal=sig)
        self._running = False

    def _delivery_report(self, err, msg) -> None:
        if err is not None:
            self._messages_failed += 1
            logger.warning("delivery_failed", error=str(err), topic=msg.topic())
        else:
            self._messages_sent += 1

    def _next_rate(self, currency: str) -> Dict[str, Any]:
        """Advance the random walk one step and return the rate event."""
        import random

        current = self._rates[currency]

        # Random walk: ~0.05% drift per tick, ±0.1% noise
        drift = random.gauss(0, 0.001)
        new_rate = current * (1 + drift)
        self._rates[currency] = new_rate

        pct_change = (new_rate - current) / current * 100
        self._change_history[currency].append(pct_change)

        # Detect anomaly: change > 2 std devs of recent history
        is_anomaly = False
        if len(self._change_history[currency]) >= 5:
            history = list(self._change_history[currency])
            mu = mean(history)
            sigma = stdev(history) if len(history) > 1 else 0.0
            if sigma > 0 and abs(pct_change - mu) > _ANOMALY_STD_THRESHOLD * sigma:
                is_anomaly = True

        spread = current * 0.002  # ~0.2% spread
        return {
            "currency_pair": f"{currency}BRL",
            "currency": currency,
            "rate": round(new_rate, 6),
            "bid": round(new_rate - spread / 2, 6),
            "ask": round(new_rate + spread / 2, 6),
            "pct_change": round(pct_change, 6),
            "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "is_anomaly": is_anomaly,
            "source": "simulator",
        }

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def publish_snapshot(self) -> None:
        """Publish current rates for all currency pairs once."""
        for currency in _BASE_RATES:
            event = self._next_rate(currency)
            try:
                self._producer.produce(
                    topic=self._config.exchange_rates_topic,
                    key=event["currency_pair"],
                    value=json.dumps(event).encode("utf-8"),
                    on_delivery=self._delivery_report,
                )
                if event["is_anomaly"]:
                    logger.warning("rate_anomaly_detected", **event)
            except Exception as exc:
                logger.error("produce_error", currency=currency, error=str(exc))

        self._producer.poll(0)

    def run(self, interval_seconds: int = 5, duration_seconds: int = 0) -> None:
        """Publish rate snapshots on a fixed interval.

        Args:
            interval_seconds: Seconds between each snapshot publication.
            duration_seconds: Total run time. 0 means run until interrupted.
        """
        logger.info("starting", interval_seconds=interval_seconds,
                    duration_seconds=duration_seconds or "indefinite")

        start = time.monotonic()

        while self._running:
            self.publish_snapshot()

            elapsed = time.monotonic() - start
            if duration_seconds and elapsed >= duration_seconds:
                break

            logger.debug("snapshot_published",
                         messages_sent=self._messages_sent,
                         messages_failed=self._messages_failed)
            time.sleep(interval_seconds)

        self._producer.flush(timeout=30)
        logger.info("finished",
                    messages_sent=self._messages_sent,
                    messages_failed=self._messages_failed)


def main() -> None:
    """CLI entry point.

    Example::

        python -m ingestion.producer.exchange_rate_producer --interval 5 --duration 60
    """
    parser = ArgumentParser(description="FX Exchange Rate Producer")
    parser.add_argument("--bootstrap-servers", default="localhost:9092",
                        help="Kafka/Redpanda bootstrap servers")
    parser.add_argument("--schema-registry", default="http://localhost:8081",
                        help="Schema Registry URL")
    parser.add_argument("--interval", type=int, default=5,
                        help="Seconds between rate snapshots (default: 5)")
    parser.add_argument("--duration", type=int, default=0,
                        help="Total run duration in seconds. 0 = run until Ctrl+C")
    args = parser.parse_args()

    cfg = ProducerConfig(
        bootstrap_servers=args.bootstrap_servers,
        schema_registry_url=args.schema_registry,
    )
    ExchangeRateProducer(config=cfg).run(
        interval_seconds=args.interval,
        duration_seconds=args.duration,
    )


if __name__ == "__main__":
    main()
