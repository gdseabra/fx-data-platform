# FX Data Platform - Iceberg Catalog Setup
# Creates Iceberg tables for all three lakehouse layers.
#
# Catalog strategy (dev): one HadoopCatalog per layer, each backed by its own MinIO bucket.
#   bronze catalog → s3a://fx-datalake-bronze   (bronze.transactions)
#   silver catalog → s3a://fx-datalake-silver   (silver.transactions)
#   gold   catalog → s3a://fx-datalake-gold     (gold.daily_volume, ...)
#
# In production, swap catalog type to "glue" — one Glue catalog per environment,
# with database-level S3 locations per layer.
#
# Run: spark-submit etl/iceberg/catalog_setup.py --env dev

import logging

from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

ALL_TABLES = [
    "bronze.transactions",
    "silver.transactions",
    "gold.daily_volume",
    "gold.user_summary",
    "gold.hourly_rates",
    "gold.anomaly_summary",
]


def create_bronze_tables(spark: SparkSession) -> None:
    """Bronze layer — raw data with schema enforcement and audit columns."""
    logger.info("Creating Bronze tables...")

    spark.sql("""
        CREATE TABLE IF NOT EXISTS bronze.transactions (
            transaction_id      STRING      NOT NULL,
            user_id             STRING      NOT NULL,
            timestamp           TIMESTAMP   NOT NULL,
            transaction_type    STRING      NOT NULL,
            currency            STRING      NOT NULL,
            amount_brl          DECIMAL(15, 2) NOT NULL,
            amount_foreign      DECIMAL(15, 4) NOT NULL,
            exchange_rate       DECIMAL(10, 4) NOT NULL,
            spread_pct          DECIMAL(5,  3) NOT NULL,
            fee_brl             DECIMAL(10, 2),
            status              STRING,
            channel             STRING,
            device              STRING,
            is_anomaly          BOOLEAN,
            anomaly_score       DECIMAL(5, 4),
            _bronze_loaded_at   TIMESTAMP,
            _source_file        STRING,
            _batch_id           STRING
        )
        USING ICEBERG
        PARTITIONED BY (days(timestamp))
        TBLPROPERTIES (
            'write.format.default'          = 'parquet',
            'write.metadata.compression-codec' = 'gzip',
            'commit.retry.num-retries'      = '3'
        )
    """)
    logger.info("  Created bronze.transactions")


def create_silver_tables(spark: SparkSession) -> None:
    """Silver layer — cleaned, enriched, validated data."""
    logger.info("Creating Silver tables...")

    spark.sql("""
        CREATE TABLE IF NOT EXISTS silver.transactions (
            transaction_id      STRING      NOT NULL,
            user_id             STRING      NOT NULL,
            timestamp           TIMESTAMP   NOT NULL,
            transaction_type    STRING      NOT NULL,
            currency            STRING      NOT NULL,
            amount_brl          DECIMAL(15, 2) NOT NULL,
            amount_foreign      DECIMAL(15, 4) NOT NULL,
            exchange_rate       DECIMAL(10, 4) NOT NULL,
            spread_pct          DECIMAL(5,  3) NOT NULL,
            fee_brl             DECIMAL(10, 2),
            status              STRING,
            channel             STRING,
            device              STRING,
            is_anomaly          BOOLEAN,
            anomaly_score       DECIMAL(5, 4),
            amount_usd          DECIMAL(15, 2),
            day_of_week         INT,
            hour_of_day         INT,
            is_business_hours   BOOLEAN,
            date                DATE,
            year_month          INT,
            _silver_loaded_at   TIMESTAMP
        )
        USING ICEBERG
        PARTITIONED BY (date, bucket(16, currency))
        TBLPROPERTIES (
            'write.format.default'     = 'parquet',
            'commit.retry.num-retries' = '3'
        )
    """)
    logger.info("  Created silver.transactions")


def create_gold_tables(spark: SparkSession) -> None:
    """Gold layer — aggregated, analytics-ready tables."""
    logger.info("Creating Gold tables...")

    spark.sql("""
        CREATE TABLE IF NOT EXISTS gold.daily_volume (
            date                DATE        NOT NULL,
            currency            STRING      NOT NULL,
            total_volume_brl    DECIMAL(18, 2),
            transaction_count   LONG,
            avg_amount          DOUBLE,
            min_amount          DECIMAL(15, 2),
            max_amount          DECIMAL(15, 2),
            p95_amount          DOUBLE,
            volume_change_pct   DOUBLE,
            ma7_volume_brl      DOUBLE
        )
        USING ICEBERG
        PARTITIONED BY (date)
        TBLPROPERTIES ('write.format.default' = 'parquet')
    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS gold.user_summary (
            user_id                 STRING  NOT NULL,
            total_transactions      LONG,
            total_volume_brl        DECIMAL(18, 2),
            avg_transaction_brl     DOUBLE,
            first_transaction_at    TIMESTAMP,
            last_transaction_at     TIMESTAMP
        )
        USING ICEBERG
        TBLPROPERTIES ('write.format.default' = 'parquet')
    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS gold.hourly_rates (
            hour_bucket         LONG    NOT NULL,
            currency            STRING  NOT NULL,
            avg_rate            DOUBLE,
            min_rate            DECIMAL(10, 4),
            max_rate            DECIMAL(10, 4),
            volatility          DOUBLE,
            transaction_count   LONG
        )
        USING ICEBERG
        TBLPROPERTIES ('write.format.default' = 'parquet')
    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS gold.anomaly_summary (
            date                DATE    NOT NULL,
            total_transactions  LONG,
            total_anomalies     LONG,
            anomaly_rate        DOUBLE
        )
        USING ICEBERG
        PARTITIONED BY (date)
        TBLPROPERTIES ('write.format.default' = 'parquet')
    """)

    logger.info("  Created gold.{daily_volume, user_summary, hourly_rates, anomaly_summary}")


def main() -> None:
    import argparse
    from etl.common.spark_session import create_spark_session

    parser = argparse.ArgumentParser(description="Iceberg Catalog Setup")
    parser.add_argument("--env", default="dev", choices=["dev", "staging", "prod"])
    args = parser.parse_args()

    spark = create_spark_session("iceberg-setup", env=args.env)
    try:
        create_bronze_tables(spark)
        create_silver_tables(spark)
        create_gold_tables(spark)
        logger.info("Iceberg catalog initialised successfully")
    finally:
        spark.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
