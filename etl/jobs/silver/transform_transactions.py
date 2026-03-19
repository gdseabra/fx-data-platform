# FX Data Platform - Silver Transformation Job
# Cleans, validates, and enriches Bronze data into the Silver Iceberg layer.
# Operations: filter invalid records, enrich with derived columns, quality checks.
#
# Run: spark-submit etl/jobs/silver/transform_transactions.py --env dev

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    dayofweek,
    hour,
    month,
    upper,
    when,
    year,
)
from pyspark.sql.functions import (
    round as spark_round,
)

logger = logging.getLogger(__name__)

# Valid ISO-4217 currency codes handled by this platform
VALID_CURRENCIES = {"USD", "EUR", "GBP", "JPY", "AUD", "CAD", "CNY", "CHF", "MXN", "ARS"}


class SilverTransformationJob:
    """Transforms Bronze data into the Silver layer."""

    def __init__(self, spark: SparkSession, env: str = "dev"):
        self.spark = spark
        self.env = env
        self.source_table = "bronze.transactions"
        self.target_table = "silver.transactions"

    # ── read ───────────────────────────────────────────────────────────────────

    def read_bronze(self) -> DataFrame:
        logger.info(f"Reading {self.source_table}...")
        return self.spark.table(self.source_table)

    # ── clean ──────────────────────────────────────────────────────────────────

    def clean(self, df: DataFrame) -> DataFrame:
        """Remove invalid records and normalise currency codes."""
        logger.info("Cleaning data...")

        df_clean = (
            df
            .withColumn("currency", upper(col("currency")))
            .filter(col("amount_brl") > 0)
            .filter(col("currency").isin(*VALID_CURRENCIES))
            .filter(col("timestamp").isNotNull())
            .filter(col("transaction_id").isNotNull())
            .filter(col("user_id").isNotNull())
        )

        logger.info(f"Cleaned to {df_clean.count()} records")
        return df_clean

    # ── enrich ─────────────────────────────────────────────────────────────────

    def enrich(self, df: DataFrame) -> DataFrame:
        """Add derived columns for analytics."""
        logger.info("Enriching data...")

        return (
            df
            # Approximate USD conversion (static rate for dev; replace with lookup in prod)
            .withColumn("amount_usd", spark_round(col("amount_brl") / 5.0, 2))
            # Temporal features
            .withColumn("day_of_week", dayofweek(col("timestamp")))
            .withColumn("hour_of_day", hour(col("timestamp")))
            .withColumn(
                "is_business_hours",
                when(
                    (col("hour_of_day") >= 9)
                    & (col("hour_of_day") <= 17)
                    & (col("day_of_week").between(2, 6)),  # Mon-Fri
                    True,
                ).otherwise(False),
            )
            # Partition keys
            .withColumn("date", col("timestamp").cast("date"))
            .withColumn(
                "year_month",
                year(col("timestamp")) * 100 + month(col("timestamp")),
            )
            # Audit
            .withColumn("_silver_loaded_at", current_timestamp())
        )

    # ── quality checks ─────────────────────────────────────────────────────────

    def validate(self, df: DataFrame) -> dict:
        """Inline quality checks; raises if critical thresholds are breached."""
        logger.info("Running quality checks...")

        total = df.count()
        checks = {
            "null_amount_brl": df.filter(col("amount_brl").isNull()).count(),
            "negative_amounts": df.filter(col("amount_brl") <= 0).count(),
            "null_timestamps": df.filter(col("timestamp").isNull()).count(),
            "invalid_currencies": df.filter(
                ~col("currency").isin(*VALID_CURRENCIES)
            ).count(),
        }

        for check, bad_count in checks.items():
            pct = bad_count / total * 100 if total > 0 else 0
            if pct > 5:
                raise ValueError(
                    f"Quality check FAILED: {check} = {bad_count}/{total} ({pct:.1f}%)"
                )
            if bad_count > 0:
                logger.warning(f"Warning: {check}: {bad_count} rows ({pct:.1f}%)")

        logger.info("All quality checks passed")
        return {k: int(v) for k, v in checks.items()}

    # ── write ──────────────────────────────────────────────────────────────────

    def write_iceberg(self, df: DataFrame) -> None:
        logger.info(f"Writing to {self.target_table}...")
        # Drop bronze-layer audit columns not present in the silver schema
        bronze_audit_cols = ["_bronze_loaded_at", "_source_file", "_batch_id"]
        df_silver = df.drop(*[c for c in bronze_audit_cols if c in df.columns])
        (
            df_silver.writeTo(self.target_table)
            .tableProperty("write.format.default", "parquet")
            .overwritePartitions()
        )

    # ── orchestrate ────────────────────────────────────────────────────────────

    def process(self) -> dict:
        metrics = {
            "rows_read": 0,
            "rows_cleaned": 0,
            "rows_written": 0,
            "status": "failed",
        }
        try:
            df = self.read_bronze()
            metrics["rows_read"] = df.count()

            df = self.clean(df)
            metrics["rows_cleaned"] = df.count()

            df = self.enrich(df)
            metrics["quality_checks"] = self.validate(df)

            self.write_iceberg(df)
            metrics["rows_written"] = metrics["rows_cleaned"]
            metrics["status"] = "success"
            logger.info(f"Silver transformation completed: {metrics}")

        except Exception as e:
            logger.error(f"Silver job failed: {e}")
            metrics["error"] = str(e)

        return metrics


def main() -> None:
    import argparse

    from etl.common.spark_session import create_spark_session

    parser = argparse.ArgumentParser(description="FX Silver Transformation Job")
    parser.add_argument("--env", default="dev", choices=["dev", "staging", "prod"])
    args = parser.parse_args()

    spark = create_spark_session("fx-silver-transformation", env=args.env)
    try:
        job = SilverTransformationJob(spark, env=args.env)
        metrics = job.process()
        print(f"\nJob Metrics: {metrics}")
    finally:
        spark.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
