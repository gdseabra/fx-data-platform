# FX Data Platform - Iceberg Maintenance
# Snapshot expiration, orphan file cleanup, and small-file compaction.
# Called daily by the Airflow iceberg_maintenance task.

import logging

from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

_ALL_TABLES = [
    "bronze.transactions",
    "silver.transactions",
    "gold.daily_volume",
    "gold.user_summary",
    "gold.hourly_rates",
    "gold.anomaly_summary",
]

_COMPACTION_TABLES = [
    "bronze.transactions",
    "silver.transactions",
]


def _get_or_create_spark(spark: SparkSession | None = None) -> SparkSession:
    if spark is not None:
        return spark
    from etl.common.spark_session import create_spark_session
    return create_spark_session("iceberg-maintenance")


def expire_snapshots(
    days: int = 7,
    spark: SparkSession | None = None,
) -> None:
    """Expire Iceberg snapshots older than *days* days.

    Iceberg retains the minimum snapshots required for time-travel within the
    retention window; older snapshots (and their data files if not shared) are
    deleted.
    """
    logger.info(f"Expiring snapshots older than {days} days...")
    _spark = _get_or_create_spark(spark)
    for table in _ALL_TABLES:
        try:
            _spark.sql(f"""
                CALL spark_catalog.system.expire_snapshots(
                    table           => '{table}',
                    older_than      => TIMESTAMP '{_days_ago_ts(days)}',
                    retain_last     => 1
                )
            """)
            logger.info(f"  Expired snapshots for {table}")
        except Exception as exc:
            logger.warning(f"  Could not expire snapshots for {table}: {exc}")


def remove_orphan_files(spark: SparkSession | None = None) -> None:
    """Remove data files on storage that are no longer referenced by any snapshot."""
    logger.info("Removing orphan files...")
    _spark = _get_or_create_spark(spark)

    for table in _ALL_TABLES:
        try:
            _spark.sql(f"""
                CALL spark_catalog.system.remove_orphan_files(table => '{table}')
            """)
            logger.info(f"  Cleaned orphan files for {table}")
        except Exception as exc:
            logger.warning(f"  Could not clean orphan files for {table}: {exc}")


def compact_small_files(spark: SparkSession | None = None) -> None:
    """Rewrite small files to improve scan performance.

    Uses binpack strategy: merges files below target_file_size_bytes (128 MB)
    into right-sized Parquet files without changing data ordering.
    """
    logger.info("Compacting small files...")
    _spark = _get_or_create_spark(spark)

    for table in _COMPACTION_TABLES:
        try:
            _spark.sql(f"""
                CALL spark_catalog.system.rewrite_data_files(
                    table                  => '{table}',
                    strategy               => 'binpack',
                    options                => map(
                        'target-file-size-bytes', '134217728',
                        'min-file-size-bytes',    '33554432'
                    )
                )
            """)
            logger.info(f"  Compacted files for {table}")
        except Exception as exc:
            logger.warning(f"  Could not compact files for {table}: {exc}")


def _days_ago_ts(days: int) -> str:
    """Return an ISO-8601 timestamp string *days* ago (UTC)."""
    from datetime import datetime, timedelta, timezone
    dt = datetime.now(timezone.utc) - timedelta(days=days)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def run_all(spark: SparkSession | None = None) -> None:
    """Run the full maintenance suite in order."""
    _spark = _get_or_create_spark(spark)
    try:
        expire_snapshots(days=7, spark=_spark)
        remove_orphan_files(spark=_spark)
        compact_small_files(spark=_spark)
        logger.info("Iceberg maintenance completed successfully")
    except Exception as exc:
        logger.error(f"Iceberg maintenance failed: {exc}")
        raise


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    run_all()


if __name__ == "__main__":
    main()
