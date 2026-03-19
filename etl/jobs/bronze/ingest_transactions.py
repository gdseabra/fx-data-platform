# FX Data Platform - Bronze Ingestion Job
# Reads raw Parquet data from the raw layer and creates the Bronze Iceberg table.
# Operations: schema enforcement, deduplication, audit columns.
#
# Run: spark-submit etl/jobs/bronze/ingest_transactions.py --date 2024-01-15 --env dev

import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, desc, row_number, current_timestamp, input_file_name,
)
from pyspark.sql.types import (
    BooleanType, DecimalType, StringType, StructField, StructType, TimestampType,
)
from pyspark.sql.window import Window

logger = logging.getLogger(__name__)


class BronzeIngestionJob:
    """Ingests raw Parquet data into the Bronze Iceberg layer."""

    # Schema matches what Redpanda Connect writes to the raw bucket
    RAW_SCHEMA = StructType([
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
        StructField("is_anomaly", BooleanType(), True),
        StructField("anomaly_score", DecimalType(5, 4), True),
    ])

    def __init__(self, spark: SparkSession, env: str = "dev"):
        self.spark = spark
        self.env = env
        self.raw_base = "s3a://fx-datalake-raw"
        self.target_table = "bronze.transactions"

    # ── read ───────────────────────────────────────────────────────────────────

    def read_raw(self, date_str: Optional[str] = None) -> DataFrame:
        """Read raw JSON for a given date (defaults to yesterday)."""
        if date_str is None:
            date_str = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")

        path = f"{self.raw_base}/events/event_date={date_str}"
        logger.info(f"Reading raw data from {path}")

        df = self.spark.read.schema(self.RAW_SCHEMA).json(path)
        n = df.count()
        logger.info(f"Read {n} raw records")
        return df

    # ── transform ──────────────────────────────────────────────────────────────

    def deduplicate(self, df: DataFrame) -> DataFrame:
        """Keep the latest record per transaction_id (idempotent re-runs)."""
        logger.info("Deduplicating transactions...")
        w = Window.partitionBy("transaction_id").orderBy(desc("timestamp"))
        df_dedup = (
            df.withColumn("_rn", row_number().over(w))
            .filter(col("_rn") == 1)
            .drop("_rn")
        )
        logger.info(f"Deduplicated to {df_dedup.count()} records")
        return df_dedup

    def add_audit_columns(self, df: DataFrame) -> DataFrame:
        """Add Bronze audit columns."""
        return (
            df
            .withColumn("_bronze_loaded_at", current_timestamp())
            .withColumn("_source_file", input_file_name())
            # batch_id = last 8 chars of filename (timestamp suffix from Connect)
            .withColumn(
                "_batch_id",
                col("_source_file").substr(-12, 8),
            )
        )

    # ── write ──────────────────────────────────────────────────────────────────

    def write_iceberg(self, df: DataFrame) -> None:
        """Append-overwrite the partition (idempotent for daily re-runs)."""
        logger.info(f"Writing to {self.target_table}...")
        (
            df.writeTo(self.target_table)
            .tableProperty("write.format.default", "parquet")
            .tableProperty("write.metadata.compression-codec", "gzip")
            .overwritePartitions()
        )

    # ── orchestrate ────────────────────────────────────────────────────────────

    def process(self, date_str: Optional[str] = None) -> dict:
        metrics = {
            "date": date_str,
            "rows_read": 0,
            "rows_deduplicated": 0,
            "rows_written": 0,
            "status": "failed",
        }
        try:
            df_raw = self.read_raw(date_str)
            metrics["rows_read"] = df_raw.count()

            df_dedup = self.deduplicate(df_raw)
            metrics["rows_deduplicated"] = df_dedup.count()

            df_final = self.add_audit_columns(df_dedup)
            self.write_iceberg(df_final)

            metrics["rows_written"] = metrics["rows_deduplicated"]
            metrics["status"] = "success"
            logger.info(f"✓ Bronze ingestion completed: {metrics}")

        except Exception as e:
            logger.error(f"✗ Bronze job failed: {e}")
            metrics["error"] = str(e)

        return metrics


def main() -> None:
    import argparse
    import sys
    from etl.common.spark_session import create_spark_session

    parser = argparse.ArgumentParser(description="FX Bronze Ingestion Job")
    parser.add_argument("--date", help="Date to process (YYYY-MM-DD)")
    parser.add_argument("--env", default="dev", choices=["dev", "staging", "prod"])
    args = parser.parse_args()

    spark = create_spark_session("fx-bronze-ingestion", env=args.env)
    try:
        job = BronzeIngestionJob(spark, env=args.env)
        metrics = job.process(date_str=args.date)
        print(f"\nJob Metrics: {metrics}", flush=True)
        if metrics.get("error"):
            sys.stderr.write(f"ERROR: {metrics['error']}\n")
            sys.stderr.flush()
            sys.exit(1)
    finally:
        sys.stdout.flush()
        sys.stderr.flush()
        spark.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
