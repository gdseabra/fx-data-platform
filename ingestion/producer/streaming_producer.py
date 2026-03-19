# FX Data Platform - Redpanda Streaming Producer
# Produces simulated FX transactions to Kafka-compatible Redpanda broker
# Supports throughput control, anomaly injection, and delivery confirmation

import json
import random
import signal
import time
from argparse import ArgumentParser
from datetime import datetime, timezone
from typing import Any, Dict

import logging

import structlog
from confluent_kafka import Producer

from ingestion.producer.config import ProducerConfig

logging.basicConfig(level=logging.INFO, format="%(message)s")

# Configure structured logging
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
        structlog.processors.JSONRenderer()
    ],
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger(__name__)


class StreamingProducer:
    """Produces simulated FX transactions to Redpanda."""

    def __init__(
        self,
        config: ProducerConfig | None = None,
        rate_msgs_per_sec: int | None = None,
        anomaly_rate: float | None = None,
        enable_anomalies: bool = False,
    ):
        """Initialize the producer.

        Args:
            config: ProducerConfig instance (uses defaults if None)
            rate_msgs_per_sec: Override for messages per second
            anomaly_rate: Override for anomaly rate (0-1)
            enable_anomalies: Whether to inject anomalies
        """
        self._config = config or ProducerConfig()
        self.rate_msgs_per_sec = rate_msgs_per_sec or self._config.default_rate_msgs_per_sec
        self.anomaly_rate = anomaly_rate or self._config.default_anomaly_rate
        self.enable_anomalies = enable_anomalies
        self.messages_sent = 0
        self.messages_failed = 0
        self.running = True

        self.producer = Producer(self._config.get_producer_config())
        logger.info("producer_initialized", bootstrap_servers=self._config.bootstrap_servers)

        # Signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, sig, frame):
        """Handle shutdown signals."""
        logger.info("shutdown_signal_received", signal=sig)
        self.running = False

    def _delivery_report(self, err, msg):
        """Delivery report callback."""
        if err is not None:
            self.messages_failed += 1
            logger.warning("message_delivery_failed", error=str(err), topic=msg.topic())
        else:
            self.messages_sent += 1

    def generate_transaction(self) -> Dict[str, Any]:
        """Generate a simulated FX transaction.

        Returns:
            Dictionary representing a transaction
        """
        is_anomaly = self.enable_anomalies and random.random() < self.anomaly_rate

        # Realistic FX transaction amounts
        base_amount = random.choice([500, 1000, 2500, 5000, 10000, 25000])
        if is_anomaly:
            # Generate anomalous amounts (very large or very small)
            base_amount = (
                random.choice([50000, 100000, 250000])
                if random.random() > 0.5
                else random.choice([10, 25, 50])
            )

        currencies = ["USD", "EUR", "GBP", "JPY", "AUD", "CAD"]
        channels = ["mobile", "web", "api"]
        tiers = ["standard", "gold", "platinum"]

        return {
            "transaction_id": str(random.randint(10**15, 10**16)),
            "user_id": str(random.randint(1, 10)),
            "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "transaction_type": random.choice(["BUY", "SELL"]),
            "currency": random.choice(currencies),
            "amount_brl": base_amount,
            "amount_foreign": base_amount / random.uniform(4.5, 5.5),
            "exchange_rate": random.uniform(4.5, 5.5),
            "spread_pct": random.uniform(0.1, 0.5),
            "fee_brl": base_amount * random.uniform(0.001, 0.01),
            "status": "completed",
            "channel": random.choice(channels),
            "device": f"device_{random.randint(1000, 9999)}",
            "is_anomaly": is_anomaly,
            "anomaly_score": random.uniform(0.7, 1.0) if is_anomaly else random.uniform(0, 0.3),
        }

    def produce_messages(self, duration_seconds: int = 60):
        """Produce messages for specified duration.

        Args:
            duration_seconds: How long to produce messages
        """
        logger.info("starting_production", duration_seconds=duration_seconds)
        start_time = time.time()
        message_count = 0

        interval = 1.0 / self.rate_msgs_per_sec if self.rate_msgs_per_sec > 0 else 0

        while self.running and (time.time() - start_time) < duration_seconds:
            transaction = self.generate_transaction()

            try:
                self.producer.produce(
                    topic=self._config.transactions_topic,
                    key=transaction["user_id"],
                    value=json.dumps(transaction).encode("utf-8"),
                    on_delivery=self._delivery_report,
                )

                message_count += 1

                # Log every 100 messages
                if message_count % 100 == 0:
                    logger.info(
                        "production_progress",
                        messages_produced=message_count,
                        messages_sent=self.messages_sent,
                        messages_failed=self.messages_failed,
                    )

                # Throttle based on rate
                if interval > 0:
                    time.sleep(interval)

            except Exception as e:
                logger.error("production_error", error=str(e))
                self.running = False

        # Flush remaining messages
        self.producer.flush(timeout=60)

        logger.info(
            "production_completed",
            total_messages=message_count,
            sent=self.messages_sent,
            failed=self.messages_failed,
        )


def main():
    """Main entry point."""
    parser = ArgumentParser(description="FX Transaction Streaming Producer")
    parser.add_argument(
        "--bootstrap-servers",
        default="localhost:9092",
        help="Kafka bootstrap servers",
    )
    parser.add_argument(
        "--schema-registry",
        default="http://localhost:8081",
        help="Schema Registry URL",
    )
    parser.add_argument(
        "--rate",
        type=int,
        default=100,
        help="Messages per second",
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=60,
        help="Duration in seconds",
    )
    parser.add_argument(
        "--anomalies",
        action="store_true",
        help="Enable anomaly injection",
    )
    parser.add_argument(
        "--anomaly-rate",
        type=float,
        default=0.05,
        help="Anomaly rate (0-1)",
    )

    args = parser.parse_args()

    cfg = ProducerConfig(
        bootstrap_servers=args.bootstrap_servers,
        schema_registry_url=args.schema_registry,
    )

    producer = StreamingProducer(
        config=cfg,
        rate_msgs_per_sec=args.rate,
        anomaly_rate=args.anomaly_rate,
        enable_anomalies=args.anomalies,
    )

    producer.produce_messages(duration_seconds=args.duration)


if __name__ == "__main__":
    main()
