# FX Data Platform - Data Schemas
# Centralized schema definitions for all data entities

from pyspark.sql.types import (
    DecimalType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


class Schemas:
    """Container for all data schemas."""

    TRANSACTION = StructType([
        StructField("transaction_id", StringType(), False),
        StructField("user_id", StringType(), False),
        StructField("timestamp", TimestampType(), False),
        StructField("transaction_type", StringType(), False),
        StructField("currency", StringType(), False),
        StructField("amount_brl", DecimalType(15, 2), False),
        StructField("amount_foreign", DecimalType(15, 4), False),
        StructField("exchange_rate", DecimalType(10, 4), False),
        StructField("spread_pct", DecimalType(5, 3), False),
        StructField("fee_brl", DecimalType(10, 2), True),
        StructField("status", StringType(), True),
        StructField("channel", StringType(), True),
        StructField("device", StringType(), True),
    ])

    USER = StructType([
        StructField("user_id", StringType(), False),
        StructField("name", StringType(), False),
        StructField("cpf", StringType(), True),
        StructField("email", StringType(), False),
        StructField("state", StringType(), True),
        StructField("tier", StringType(), True),
        StructField("registration_date", TimestampType(), True),
        StructField("avg_monthly_volume", DecimalType(15, 2), True),
    ])

    EXCHANGE_RATE = StructType([
        StructField("rate_id", StringType(), False),
        StructField("currency_pair", StringType(), False),
        StructField("rate", DecimalType(10, 4), False),
        StructField("bid", DecimalType(10, 4), True),
        StructField("ask", DecimalType(10, 4), True),
        StructField("timestamp", TimestampType(), False),
    ])
