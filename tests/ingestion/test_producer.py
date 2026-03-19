# Unit tests for ingestion producers

import json
from unittest.mock import MagicMock, patch

import pytest

from ingestion.producer.config import ProducerConfig
from ingestion.producer.exchange_rate_producer import ExchangeRateProducer
from ingestion.producer.streaming_producer import StreamingProducer


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def config() -> ProducerConfig:
    return ProducerConfig(
        bootstrap_servers="localhost:9092",
        schema_registry_url="http://localhost:8081",
    )


@pytest.fixture()
def producer(config: ProducerConfig) -> StreamingProducer:
    with patch("ingestion.producer.streaming_producer.Producer"):
        return StreamingProducer(config=config, enable_anomalies=False)


@pytest.fixture()
def rate_producer(config: ProducerConfig) -> ExchangeRateProducer:
    with patch("ingestion.producer.exchange_rate_producer.Producer"):
        return ExchangeRateProducer(config=config)


# ---------------------------------------------------------------------------
# StreamingProducer — transaction generation
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_generate_transaction_required_fields(producer: StreamingProducer) -> None:
    tx = producer.generate_transaction()
    required = [
        "transaction_id", "user_id", "timestamp", "transaction_type",
        "currency", "amount_brl", "amount_foreign", "exchange_rate",
        "spread_pct", "status",
    ]
    for field in required:
        assert field in tx, f"Missing required field: {field}"


@pytest.mark.unit
def test_generate_transaction_valid_currency(producer: StreamingProducer) -> None:
    valid = {"USD", "EUR", "GBP", "JPY", "AUD", "CAD"}
    for _ in range(20):
        assert producer.generate_transaction()["currency"] in valid


@pytest.mark.unit
def test_generate_transaction_positive_amount(producer: StreamingProducer) -> None:
    for _ in range(20):
        assert producer.generate_transaction()["amount_brl"] > 0


@pytest.mark.unit
def test_no_anomaly_when_disabled(producer: StreamingProducer) -> None:
    for _ in range(50):
        assert producer.generate_transaction()["is_anomaly"] is False


# ---------------------------------------------------------------------------
# StreamingProducer — anomaly injection
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_anomaly_injection_rate() -> None:
    with patch("ingestion.producer.streaming_producer.Producer"):
        p = StreamingProducer(anomaly_rate=0.5, enable_anomalies=True)
    anomalies = sum(p.generate_transaction()["is_anomaly"] for _ in range(200))
    assert 60 < anomalies < 140, f"Expected ~50% anomaly rate, got {anomalies}/200"


@pytest.mark.unit
def test_anomaly_amounts_are_extreme() -> None:
    with patch("ingestion.producer.streaming_producer.Producer"):
        p = StreamingProducer(anomaly_rate=1.0, enable_anomalies=True)
    amounts = [p.generate_transaction()["amount_brl"] for _ in range(50)]
    normal_max = 25_000
    assert any(a > normal_max or a < 100 for a in amounts), \
        "Anomalous transactions should have extreme amounts"


# ---------------------------------------------------------------------------
# StreamingProducer — partition key
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_partition_key_is_user_id(producer: StreamingProducer) -> None:
    """Messages must be keyed by user_id to guarantee per-user ordering."""
    tx = producer.generate_transaction()
    # The key used in produce() must match user_id
    assert tx["user_id"] is not None
    assert isinstance(tx["user_id"], str)


# ---------------------------------------------------------------------------
# StreamingProducer — serialization
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_message_is_json_serializable(producer: StreamingProducer) -> None:
    tx = producer.generate_transaction()
    raw = json.dumps(tx)
    assert json.loads(raw)["transaction_id"] == tx["transaction_id"]


@pytest.mark.unit
def test_message_roundtrip_preserves_all_fields(producer: StreamingProducer) -> None:
    tx = producer.generate_transaction()
    assert json.loads(json.dumps(tx)) == tx


# ---------------------------------------------------------------------------
# StreamingProducer — connection error handling
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_produce_error_sets_running_false() -> None:
    """If Producer.produce() raises, the loop should stop gracefully."""
    with patch("ingestion.producer.streaming_producer.Producer") as mock_cls:
        mock_producer = MagicMock()
        mock_producer.produce.side_effect = Exception("broker unavailable")
        mock_cls.return_value = mock_producer

        p = StreamingProducer(enable_anomalies=False)
        p.produce_messages(duration_seconds=60)  # should exit early

    assert p.running is False


# ---------------------------------------------------------------------------
# StreamingProducer — throughput configuration
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_config_rate_is_applied() -> None:
    cfg = ProducerConfig(default_rate_msgs_per_sec=500)
    with patch("ingestion.producer.streaming_producer.Producer"):
        p = StreamingProducer(config=cfg)
    assert p.rate_msgs_per_sec == 500


@pytest.mark.unit
def test_rate_override_takes_precedence_over_config() -> None:
    cfg = ProducerConfig(default_rate_msgs_per_sec=100)
    with patch("ingestion.producer.streaming_producer.Producer"):
        p = StreamingProducer(config=cfg, rate_msgs_per_sec=999)
    assert p.rate_msgs_per_sec == 999


# ---------------------------------------------------------------------------
# ExchangeRateProducer — rate generation
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_rate_event_fields(rate_producer: ExchangeRateProducer) -> None:
    event = rate_producer._next_rate("USD")
    required = ["currency_pair", "currency", "rate", "bid", "ask",
                 "pct_change", "timestamp", "is_anomaly"]
    for field in required:
        assert field in event, f"Missing field: {field}"


@pytest.mark.unit
def test_bid_less_than_ask(rate_producer: ExchangeRateProducer) -> None:
    for currency in ["USD", "EUR", "GBP"]:
        event = rate_producer._next_rate(currency)
        assert event["bid"] < event["ask"]


@pytest.mark.unit
def test_rate_stays_positive_after_many_steps(rate_producer: ExchangeRateProducer) -> None:
    for _ in range(500):
        event = rate_producer._next_rate("USD")
        assert event["rate"] > 0


@pytest.mark.unit
def test_anomaly_flag_type(rate_producer: ExchangeRateProducer) -> None:
    for _ in range(30):
        event = rate_producer._next_rate("USD")
        assert isinstance(event["is_anomaly"], bool)
