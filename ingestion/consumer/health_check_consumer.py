# FX Data Platform - Health Check Consumer
# Consumes from Redpanda topics and reports metrics

import json
import logging
from argparse import ArgumentParser
from datetime import datetime

from confluent_kafka import Consumer, KafkaError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class HealthCheckConsumer:
    """Consumes messages and reports health/metrics."""

    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        group_id: str = "health-check-consumer",
    ):
        """Initialize consumer."""
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id

        self.consumer_config = {
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": "latest",
            "enable.auto.commit": False,
        }

        self.consumer = Consumer(self.consumer_config)

    def consume_and_report(self, duration_seconds: int = 60):
        """Consume messages and report metrics."""
        topics = ["fx.transactions", "fx.exchange-rates", "fx.users-cdc"]
        self.consumer.subscribe(topics)

        stats = {
            topic: {"count": 0, "last_msg": None} for topic in topics
        }

        logger.info(f"Consuming from topics: {topics} for {duration_seconds}s")

        start_time = datetime.utcnow()

        try:
            while (datetime.utcnow() - start_time).total_seconds() < duration_seconds:
                msg = self.consumer.poll(timeout=1)

                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    logger.error(f"Consumer error: {msg.error()}")
                    continue

                topic = msg.topic()
                stats[topic]["count"] += 1
                stats[topic]["last_msg"] = msg.value().decode("utf-8")

                # Log every 100 messages
                if stats[topic]["count"] % 100 == 0:
                    logger.info(
                        f"{topic}: {stats[topic]['count']} messages received, "
                        f"lag: {msg.offset()}"
                    )

        except KeyboardInterrupt:
            logger.info("Interrupted by user")
        finally:
            self.consumer.close()

        # Print final stats
        logger.info("\n=== Health Check Report ===")
        for topic, data in stats.items():
            logger.info(f"{topic}: {data['count']} messages")
            if data["last_msg"]:
                logger.info(f"  Last message: {data['last_msg'][:100]}...")


def main():
    """Main entry point."""
    parser = ArgumentParser(description="FX Health Check Consumer")
    parser.add_argument(
        "--bootstrap-servers",
        default="localhost:9092",
        help="Kafka bootstrap servers",
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=60,
        help="Duration in seconds",
    )

    args = parser.parse_args()

    consumer = HealthCheckConsumer(bootstrap_servers=args.bootstrap_servers)
    consumer.consume_and_report(duration_seconds=args.duration)


if __name__ == "__main__":
    main()
