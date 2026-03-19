# ml/feature_store/feature_repo/data_sources.py
# Data sources for the Feast feature store.
#
# Dev:  local Parquet files written by the ETL gold/silver jobs.
# Prod: replace FileSource with SparkSource pointing at the Glue Catalog tables
#       (fx_gold.user_summary, fx_gold.hourly_rates, fx_silver.transactions).

import os

from feast import FileSource
from feast.data_format import ParquetFormat

# ---------------------------------------------------------------------------
# Base paths — override via env vars for production
# ---------------------------------------------------------------------------
_DATA_ROOT = os.environ.get("FEAST_DATA_ROOT", "data/feature_store/sources")

# ---------------------------------------------------------------------------
# gold_user_summary  →  user_transaction_features
# ---------------------------------------------------------------------------
user_summary_source = FileSource(
    name="user_summary_source",
    path=os.path.join(_DATA_ROOT, "gold_user_summary.parquet"),
    file_format=ParquetFormat(),
    timestamp_field="last_transaction",
    description=(
        "Gold layer user summary table: aggregated transaction stats per user. "
        "Refreshed daily by the ETL DAG (gold_aggregate task)."
    ),
)

# ---------------------------------------------------------------------------
# gold_hourly_rates  →  currency_features
# ---------------------------------------------------------------------------
hourly_rates_source = FileSource(
    name="hourly_rates_source",
    path=os.path.join(_DATA_ROOT, "gold_hourly_rates.parquet"),
    file_format=ParquetFormat(),
    timestamp_field="window_start",
    description=(
        "Gold layer hourly exchange-rate aggregates per currency. "
        "Refreshed hourly by the ETL DAG."
    ),
)

# ---------------------------------------------------------------------------
# silver.transactions  →  transaction_pattern_features
# ---------------------------------------------------------------------------
silver_transactions_source = FileSource(
    name="silver_transactions_source",
    path=os.path.join(_DATA_ROOT, "silver_transactions.parquet"),
    file_format=ParquetFormat(),
    timestamp_field="timestamp",
    description=(
        "Silver layer transaction records. Used to compute per-user "
        "rolling-window pattern features (velocity, avg amount, etc.)."
    ),
)
