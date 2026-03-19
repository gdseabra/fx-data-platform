# Schema validation tests for ingestion events
# Verifies that produced messages conform to JSON Schemas and that
# malformed messages are correctly rejected.

import json
from pathlib import Path

import jsonschema
import pytest

# ---------------------------------------------------------------------------
# Load schemas once at module level
# ---------------------------------------------------------------------------

_SCHEMA_DIR = Path(__file__).parents[2] / "ingestion" / "schemas"


def _load(filename: str) -> dict:
    return json.loads((_SCHEMA_DIR / filename).read_text())


TRANSACTION_SCHEMA = _load("transaction_event.json")
USER_SCHEMA = _load("user_event.json")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _valid_transaction(**overrides) -> dict:
    base = {
        "transaction_id": "tx-abc-123",
        "user_id": "user-001",
        "timestamp": "2024-03-15T10:30:00Z",
        "transaction_type": "BUY",
        "currency": "USD",
        "amount_brl": 5000.0,
        "amount_foreign": 1000.0,
        "exchange_rate": 5.0,
        "spread_pct": 0.3,
        "fee_brl": 15.0,
        "status": "completed",
        "channel": "mobile",
        "device": "device_1234",
        "is_anomaly": False,
        "anomaly_score": 0.05,
    }
    return {**base, **overrides}


def _valid_user(**overrides) -> dict:
    base = {
        "user_id": "550e8400-e29b-41d4-a716-446655440000",
        "name": "João Silva",
        "email": "joao@example.com",
        "state": "SP",
        "tier": "gold",
        "registration_date": "2023-01-01T00:00:00Z",
    }
    return {**base, **overrides}


def _assert_valid(schema: dict, data: dict) -> None:
    jsonschema.validate(instance=data, schema=schema)


def _assert_invalid(schema: dict, data: dict) -> None:
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(instance=data, schema=schema)


# ---------------------------------------------------------------------------
# Transaction schema — valid messages
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_valid_transaction_passes() -> None:
    _assert_valid(TRANSACTION_SCHEMA, _valid_transaction())


@pytest.mark.unit
def test_transaction_buy_and_sell_are_valid() -> None:
    _assert_valid(TRANSACTION_SCHEMA, _valid_transaction(transaction_type="BUY"))
    _assert_valid(TRANSACTION_SCHEMA, _valid_transaction(transaction_type="SELL"))


@pytest.mark.unit
def test_transaction_all_channels_valid() -> None:
    for ch in ("mobile", "web", "api"):
        _assert_valid(TRANSACTION_SCHEMA, _valid_transaction(channel=ch))


@pytest.mark.unit
def test_transaction_all_statuses_valid() -> None:
    for st in ("pending", "completed", "failed", "cancelled"):
        _assert_valid(TRANSACTION_SCHEMA, _valid_transaction(status=st))


@pytest.mark.unit
def test_transaction_optional_fields_can_be_absent() -> None:
    """fee_brl, channel, device, is_anomaly, anomaly_score are optional."""
    minimal = {
        "transaction_id": "tx-001",
        "user_id": "user-001",
        "timestamp": "2024-03-15T10:30:00Z",
        "transaction_type": "BUY",
        "currency": "EUR",
        "amount_brl": 1000.0,
        "amount_foreign": 200.0,
        "exchange_rate": 5.0,
        "spread_pct": 0.3,
        "status": "completed",
    }
    _assert_valid(TRANSACTION_SCHEMA, minimal)


# ---------------------------------------------------------------------------
# Transaction schema — malformed messages
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_transaction_missing_required_field_rejected() -> None:
    for field in ("transaction_id", "user_id", "currency", "amount_brl"):
        bad = _valid_transaction()
        del bad[field]
        _assert_invalid(TRANSACTION_SCHEMA, bad)


@pytest.mark.unit
def test_transaction_invalid_currency_code_rejected() -> None:
    _assert_invalid(TRANSACTION_SCHEMA, _valid_transaction(currency="US"))   # 2 chars
    _assert_invalid(TRANSACTION_SCHEMA, _valid_transaction(currency="usd"))  # lowercase
    _assert_invalid(TRANSACTION_SCHEMA, _valid_transaction(currency="USDD")) # 4 chars


@pytest.mark.unit
def test_transaction_negative_amount_rejected() -> None:
    _assert_invalid(TRANSACTION_SCHEMA, _valid_transaction(amount_brl=-1.0))
    _assert_invalid(TRANSACTION_SCHEMA, _valid_transaction(amount_foreign=-1.0))


@pytest.mark.unit
def test_transaction_invalid_type_rejected() -> None:
    _assert_invalid(TRANSACTION_SCHEMA, _valid_transaction(transaction_type="TRANSFER"))


@pytest.mark.unit
def test_transaction_anomaly_score_out_of_range_rejected() -> None:
    _assert_invalid(TRANSACTION_SCHEMA, _valid_transaction(anomaly_score=1.5))
    _assert_invalid(TRANSACTION_SCHEMA, _valid_transaction(anomaly_score=-0.1))


@pytest.mark.unit
def test_transaction_additional_properties_rejected() -> None:
    """Schema sets additionalProperties: false."""
    _assert_invalid(TRANSACTION_SCHEMA, _valid_transaction(unknown_field="bad"))


# ---------------------------------------------------------------------------
# Transaction schema — backward compatibility
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_backward_compat_message_without_anomaly_fields() -> None:
    """Messages produced before anomaly detection was added must still be valid."""
    old_msg = _valid_transaction()
    del old_msg["is_anomaly"]
    del old_msg["anomaly_score"]
    _assert_valid(TRANSACTION_SCHEMA, old_msg)


# ---------------------------------------------------------------------------
# User schema — valid messages
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_valid_user_passes() -> None:
    _assert_valid(USER_SCHEMA, _valid_user())


@pytest.mark.unit
def test_user_all_tiers_valid() -> None:
    for tier in ("standard", "gold", "platinum"):
        _assert_valid(USER_SCHEMA, _valid_user(tier=tier))


@pytest.mark.unit
def test_user_optional_fields_absent() -> None:
    minimal = {
        "user_id": "550e8400-e29b-41d4-a716-446655440000",
        "name": "Maria",
        "email": "maria@example.com",
        "tier": "standard",
    }
    _assert_valid(USER_SCHEMA, minimal)


# ---------------------------------------------------------------------------
# User schema — malformed messages
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_user_missing_required_field_rejected() -> None:
    for field in ("user_id", "name", "email", "tier"):
        bad = _valid_user()
        del bad[field]
        _assert_invalid(USER_SCHEMA, bad)


@pytest.mark.unit
def test_user_invalid_tier_rejected() -> None:
    _assert_invalid(USER_SCHEMA, _valid_user(tier="vip"))


@pytest.mark.unit
def test_user_invalid_state_format_rejected() -> None:
    _assert_invalid(USER_SCHEMA, _valid_user(state="SP1"))   # 3 chars
    _assert_invalid(USER_SCHEMA, _valid_user(state="sp"))    # lowercase
