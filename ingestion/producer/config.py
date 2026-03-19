# FX Data Platform - Producer Configuration
# Centralized configuration for Kafka/Redpanda producers

import os
from dataclasses import dataclass


@dataclass
class ProducerConfig:
    """Configuration for Kafka producers."""

    bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    schema_registry_url: str = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")

    # Producer settings
    acks: str = "all"
    retries: int = 3
    max_in_flight_requests: int = 1
    batch_size: int = 16384
    linger_ms: int = 10

    # Throughput control
    default_rate_msgs_per_sec: int = 100
    default_anomaly_rate: float = 0.05

    # Topic names
    transactions_topic: str = "fx.transactions"
    exchange_rates_topic: str = "fx.exchange-rates"
    users_cdc_topic: str = "fx.users-cdc"

    def get_producer_config(self) -> dict:
        """Get producer configuration dictionary."""
        return {
            "bootstrap.servers": self.bootstrap_servers,
            "acks": self.acks,
            "retries": self.retries,
            "max.in.flight.requests.per.connection": self.max_in_flight_requests,
            "batch.size": self.batch_size,
            "linger.ms": self.linger_ms,
        }
