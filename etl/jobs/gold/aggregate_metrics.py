# FX Data Platform - Gold Aggregation Job
# Creates analytics-ready aggregated tables from Silver layer
# Silver → Gold (daily aggregations + window functions)
#
# Run: spark-submit etl/jobs/gold/aggregate_metrics.py --env dev

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    avg as spark_avg,
)
from pyspark.sql.functions import (
    col,
    count,
    dayofmonth,
    hour,
    lag,
    month,
    percentile_approx,
    stddev,
    to_date,
    year,
)
from pyspark.sql.functions import (
    max as spark_max,
)
from pyspark.sql.functions import (
    min as spark_min,
)
from pyspark.sql.functions import (
    sum as spark_sum,
)
from pyspark.sql.window import Window

logger = logging.getLogger(__name__)


class GoldAggregationJob:
    """Creates Gold layer aggregations for analytics."""

    def __init__(self, spark: SparkSession, env: str = "dev"):
        self.spark = spark
        self.env = env
        self.source_db = "silver"
        self.target_db = "gold"

    def _silver_df(self) -> DataFrame:
        return self.spark.table(f"{self.source_db}.transactions")

    # ── a) daily volume ────────────────────────────────────────────────────────

    def create_daily_volume(self) -> DataFrame:
        """Volume diário por moeda com variação dia-a-dia e média móvel 7 dias."""
        logger.info("Creating daily volume aggregations...")

        base = (
            self._silver_df()
            .groupBy(to_date(col("timestamp")).alias("date"), col("currency"))
            .agg(
                spark_sum("amount_brl").alias("total_volume_brl"),
                count("transaction_id").alias("transaction_count"),
                spark_avg("amount_brl").alias("avg_amount"),
                spark_min("amount_brl").alias("min_amount"),
                spark_max("amount_brl").alias("max_amount"),
                percentile_approx("amount_brl", 0.95).alias("p95_amount"),
            )
        )

        w_prev = Window.partitionBy("currency").orderBy("date")
        w_7d = Window.partitionBy("currency").orderBy("date").rowsBetween(-6, 0)

        return (
            base
            .withColumn("prev_volume_brl", lag("total_volume_brl", 1).over(w_prev))
            .withColumn(
                "volume_change_pct",
                (col("total_volume_brl") - col("prev_volume_brl"))
                / col("prev_volume_brl") * 100,
            )
            .withColumn("ma7_volume_brl", spark_avg("total_volume_brl").over(w_7d))
            .drop("prev_volume_brl")
        )

    # ── b) user summary ────────────────────────────────────────────────────────

    def create_user_summary(self) -> DataFrame:
        """Resumo por usuário."""
        logger.info("Creating user summaries...")

        return (
            self._silver_df()
            .groupBy("user_id")
            .agg(
                count("transaction_id").alias("total_transactions"),
                spark_sum("amount_brl").alias("total_volume_brl"),
                spark_avg("amount_brl").alias("avg_transaction_brl"),
                spark_min("timestamp").alias("first_transaction_at"),
                spark_max("timestamp").alias("last_transaction_at"),
            )
        )

    # ── c) hourly rates ────────────────────────────────────────────────────────

    def create_hourly_rates(self) -> DataFrame:
        """Taxa de câmbio média por hora por moeda."""
        logger.info("Creating hourly rate aggregations...")

        return (
            self._silver_df()
            .withColumn(
                "hour_bucket",
                (
                    year(col("timestamp")) * 1_000_000
                    + month(col("timestamp")) * 10_000
                    + dayofmonth(col("timestamp")) * 100
                    + hour(col("timestamp"))
                ),
            )
            .groupBy("hour_bucket", "currency")
            .agg(
                spark_avg("exchange_rate").alias("avg_rate"),
                spark_min("exchange_rate").alias("min_rate"),
                spark_max("exchange_rate").alias("max_rate"),
                stddev("exchange_rate").alias("volatility"),
                count("transaction_id").alias("transaction_count"),
            )
        )

    # ── d) anomaly summary ─────────────────────────────────────────────────────

    def create_anomaly_summary(self) -> DataFrame:
        """Contagem de anomalias por dia."""
        logger.info("Creating anomaly summary...")

        silver = self._silver_df()
        if "is_anomaly" not in silver.columns:
            logger.warning("is_anomaly column missing — skipping anomaly summary")
            return self.spark.createDataFrame(
                [],
                "date DATE, total_transactions LONG, total_anomalies LONG, anomaly_rate DOUBLE",
            )

        return (
            silver
            .groupBy(to_date(col("timestamp")).alias("date"))
            .agg(
                count("transaction_id").alias("total_transactions"),
                spark_sum(col("is_anomaly").cast("long")).alias("total_anomalies"),
            )
            .withColumn(
                "anomaly_rate",
                col("total_anomalies") / col("total_transactions"),
            )
        )

    # ── write + process ────────────────────────────────────────────────────────

    def _write(self, df: DataFrame, table: str) -> int:
        full_table = f"{self.target_db}.{table}"
        logger.info(f"Writing {full_table}...")
        (
            df.writeTo(full_table)
            .tableProperty("write.format.default", "parquet")
            .createOrReplace()
        )
        n = df.count()
        logger.info(f"✓ Wrote {n} rows to {full_table}")
        return n

    def process(self) -> dict:
        metrics: dict = {"status": "failed", "tables": {}}
        try:
            metrics["tables"]["daily_volume"] = self._write(
                self.create_daily_volume(), "daily_volume"
            )
            metrics["tables"]["user_summary"] = self._write(
                self.create_user_summary(), "user_summary"
            )
            metrics["tables"]["hourly_rates"] = self._write(
                self.create_hourly_rates(), "hourly_rates"
            )
            metrics["tables"]["anomaly_summary"] = self._write(
                self.create_anomaly_summary(), "anomaly_summary"
            )
            metrics["status"] = "success"
            logger.info(f"✓ Gold aggregation completed: {metrics}")
        except Exception as e:
            logger.error(f"✗ Gold job failed: {e}")
            metrics["error"] = str(e)
        return metrics


def main() -> None:
    import argparse

    from etl.common.spark_session import create_spark_session

    parser = argparse.ArgumentParser(description="FX Gold Aggregation Job")
    parser.add_argument("--env", default="dev", choices=["dev", "staging", "prod"])
    args = parser.parse_args()

    spark = create_spark_session("fx-gold-aggregation", env=args.env)
    try:
        job = GoldAggregationJob(spark, env=args.env)
        metrics = job.process()
        print(f"\nJob Result: {metrics}")
    finally:
        spark.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
