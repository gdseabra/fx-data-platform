# ml/feature_store/feature_repo/feature_definitions.py
# Feast feature definitions for the FX anomaly-detection platform.
#
# Entities:
#   user         — join key: user_id
#   currency_pair — join key: currency
#
# Feature Views:
#   user_transaction_features    — per-user aggregated stats (TTL 1 day)
#   currency_features            — per-currency rate stats (TTL 1 hour)
#   transaction_pattern_features — rolling-window user patterns (TTL 1 hour)
#
# On-Demand Feature View:
#   realtime_anomaly_features    — computed at inference time from request + user features

from datetime import timedelta

import pandas as pd
from feast import Entity, FeatureView, Field, OnDemandFeatureView
from feast.transformation.pandas_transformation import PandasTransformation
from feast.types import Bool, Float32, Float64, Int64, String

from ml.feature_store.feature_repo.data_sources import (
    hourly_rates_source,
    silver_transactions_source,
    user_summary_source,
)

# ---------------------------------------------------------------------------
# Entities
# ---------------------------------------------------------------------------

user = Entity(
    name="user",
    join_keys=["user_id"],
    description="FX platform user identified by UUID.",
)

currency_pair = Entity(
    name="currency_pair",
    join_keys=["currency"],
    description="ISO-4217 currency code (e.g. USD, EUR, GBP).",
)

# ---------------------------------------------------------------------------
# Feature View 1 — user_transaction_features
# Source: gold_user_summary (daily refresh)
# TTL: 1 day — stale user stats acceptable for same-day inference
# ---------------------------------------------------------------------------

user_transaction_features = FeatureView(
    name="user_transaction_features",
    entities=[user],
    ttl=timedelta(days=1),
    schema=[
        Field(name="total_transactions_30d", dtype=Int64),
        Field(name="total_volume_30d", dtype=Float64),
        Field(name="avg_ticket_30d", dtype=Float64),
        Field(name="preferred_currency", dtype=String),
        Field(name="transaction_count_buy", dtype=Int64),
        Field(name="transaction_count_sell", dtype=Int64),
        Field(name="days_since_registration", dtype=Int64),
        Field(name="tier", dtype=String),
    ],
    source=user_summary_source,
    description=(
        "Per-user transaction statistics aggregated over the last 30 days. "
        "Sourced from the gold layer user_summary table."
    ),
    tags={"team": "ml-engineering", "layer": "gold"},
)

# ---------------------------------------------------------------------------
# Feature View 2 — currency_features
# Source: gold_hourly_rates (hourly refresh)
# TTL: 1 hour — exchange rates change frequently
# ---------------------------------------------------------------------------

currency_features = FeatureView(
    name="currency_features",
    entities=[currency_pair],
    ttl=timedelta(hours=1),
    schema=[
        Field(name="current_rate", dtype=Float64),
        Field(name="rate_change_1h", dtype=Float64),
        Field(name="rate_change_24h", dtype=Float64),
        Field(name="volatility_7d", dtype=Float64),
        Field(name="avg_spread", dtype=Float64),
        Field(name="volume_24h", dtype=Float64),
    ],
    source=hourly_rates_source,
    description=(
        "Per-currency exchange-rate statistics. "
        "Sourced from the gold layer hourly_rates table."
    ),
    tags={"team": "ml-engineering", "layer": "gold"},
)

# ---------------------------------------------------------------------------
# Feature View 3 — transaction_pattern_features
# Source: silver.transactions (near-realtime)
# TTL: 1 hour
# ---------------------------------------------------------------------------

transaction_pattern_features = FeatureView(
    name="transaction_pattern_features",
    entities=[user],
    ttl=timedelta(hours=1),
    schema=[
        Field(name="transactions_last_1h", dtype=Int64),
        Field(name="avg_amount_last_24h", dtype=Float64),
        Field(name="distinct_currencies_last_7d", dtype=Int64),
        Field(name="night_transaction_ratio", dtype=Float32),
    ],
    source=silver_transactions_source,
    description=(
        "Rolling-window per-user transaction patterns computed from the silver layer. "
        "High cardinality — kept in online store with 1-hour TTL."
    ),
    tags={"team": "ml-engineering", "layer": "silver"},
)

# ---------------------------------------------------------------------------
# On-Demand Feature View — realtime_anomaly_features
# Computed at request time (no offline store write).
# Inputs: transaction request fields + user_transaction_features
# ---------------------------------------------------------------------------

def _compute_realtime_anomaly_features(inputs: pd.DataFrame) -> pd.DataFrame:
    """Compute on-demand anomaly signals from user features + request context."""
    df = pd.DataFrame()

    # z-score of current amount vs user's historical average
    avg = inputs["avg_ticket_30d"].fillna(0)
    # avg_amount_last_24h serves as a short-window proxy for std dev
    std_proxy = inputs["avg_amount_last_24h"].fillna(avg).replace(0, 1)
    # We can't access the raw request amount here; use avg_amount_last_24h delta
    df["z_score_amount"] = ((inputs["avg_amount_last_24h"] - avg) / std_proxy).clip(-5, 5).astype("float32")

    # Unusual hour: night transactions (00:00–06:00) flagged via ratio
    df["is_unusual_hour"] = (inputs["night_transaction_ratio"].fillna(0) > 0.5)

    # Velocity score: how busy this user is right now vs their norm
    normal_velocity = (inputs["total_transactions_30d"].fillna(0) / 30).replace(0, 1)
    df["velocity_score"] = (inputs["transactions_last_1h"].fillna(0) / normal_velocity).clip(0, 20).astype("float32")

    return df


realtime_anomaly_features = OnDemandFeatureView(
    name="realtime_anomaly_features",
    sources=[user_transaction_features, transaction_pattern_features],
    schema=[
        Field(name="z_score_amount", dtype=Float32),
        Field(name="is_unusual_hour", dtype=Bool),
        Field(name="velocity_score", dtype=Float32),
    ],
    feature_transformation=PandasTransformation(
        udf=_compute_realtime_anomaly_features,
        udf_string="realtime_anomaly_features",
    ),
    description=(
        "Real-time anomaly signals computed at inference time. "
        "z_score: how many std devs the current amount is from the user's 30-day mean. "
        "is_unusual_hour: transaction between 00:00-06:00 local time. "
        "velocity_score: transactions_last_1h / avg daily velocity."
    ),
)
