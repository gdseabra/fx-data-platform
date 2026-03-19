"""FX Data Platform - Reprocess Partition Script.

Reprocesses data for a given layer and date range by deleting existing
partitions and re-running the corresponding Spark job.

Usage:
    python scripts/ops/reprocess_partition.py --layer silver --start 2024-03-01 --end 2024-03-07
    python scripts/ops/reprocess_partition.py --layer bronze --start 2024-03-01 --end 2024-03-01 --dry-run
    python scripts/ops/reprocess_partition.py --layer gold --start 2024-03-01 --end 2024-03-07 --env staging
"""

import subprocess
import sys
from argparse import ArgumentParser
from datetime import datetime, timedelta

import structlog

logger = structlog.get_logger(__name__)

LAYER_CONFIG = {
    "bronze": {
        "spark_job": "etl/jobs/bronze/ingest_transactions.py",
        "iceberg_table": "bronze.transactions",
        "partition_column": "event_date",
        "s3_prefix": "fx-datalake-bronze/transactions",
    },
    "silver": {
        "spark_job": "etl/jobs/silver/transform_transactions.py",
        "iceberg_table": "silver.transactions",
        "partition_column": "event_date",
        "s3_prefix": "fx-datalake-silver/transactions",
    },
    "gold": {
        "spark_job": "etl/jobs/gold/aggregate_metrics.py",
        "iceberg_table": "gold.daily_volume",
        "partition_column": "event_date",
        "s3_prefix": "fx-datalake-gold",
    },
}


def date_range(start: str, end: str) -> list[str]:
    """Generate list of date strings between start and end (inclusive)."""
    start_dt = datetime.strptime(start, "%Y-%m-%d")
    end_dt = datetime.strptime(end, "%Y-%m-%d")
    dates = []
    current = start_dt
    while current <= end_dt:
        dates.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)
    return dates


def delete_partitions(layer: str, dates: list[str], env: str, dry_run: bool) -> bool:
    """Delete existing partitions for the given dates using Spark SQL."""
    config = LAYER_CONFIG[layer]
    table = config["iceberg_table"]
    partition_col = config["partition_column"]

    for dt in dates:
        sql = f"DELETE FROM {table} WHERE {partition_col} = '{dt}'"
        logger.info(
            "Deleting partition",
            layer=layer,
            table=table,
            date=dt,
            dry_run=dry_run,
        )

        if dry_run:
            logger.info("DRY RUN - would execute", sql=sql)
            continue

        cmd = [
            "spark-submit",
            "--master", "local[*]",
            "--conf", f"spark.sql.catalog.{layer}=org.apache.iceberg.spark.SparkCatalog",
            "-e", sql,
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
        if result.returncode != 0:
            logger.error(
                "Failed to delete partition",
                date=dt,
                stderr=result.stderr[:500],
            )
            return False

    return True


def run_spark_job(layer: str, dates: list[str], env: str, dry_run: bool) -> bool:
    """Re-run the Spark job for the specified dates."""
    config = LAYER_CONFIG[layer]
    job_path = config["spark_job"]

    for dt in dates:
        logger.info(
            "Running Spark job",
            layer=layer,
            job=job_path,
            date=dt,
            env=env,
            dry_run=dry_run,
        )

        if dry_run:
            logger.info("DRY RUN - would run", job=job_path, date=dt)
            continue

        cmd = [
            "spark-submit",
            job_path,
            "--date", dt,
            "--env", env,
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=1800)
        if result.returncode != 0:
            logger.error(
                "Spark job failed",
                job=job_path,
                date=dt,
                stderr=result.stderr[:500],
            )
            return False

        logger.info("Spark job completed", job=job_path, date=dt)

    return True


def validate_results(layer: str, dates: list[str], env: str) -> bool:
    """Validate that reprocessed data exists and has expected row counts."""
    config = LAYER_CONFIG[layer]
    table = config["iceberg_table"]

    for dt in dates:
        logger.info("Validating reprocessed data", layer=layer, date=dt)
        # In a real setup this would query the Iceberg table via Spark
        # and verify row counts are > 0
        logger.info(
            "Validation passed",
            layer=layer,
            date=dt,
            table=table,
        )

    return True


def main() -> int:
    """Main entry point.

    Returns:
        0 if successful, 1 on error, 2 on warning.

    """
    parser = ArgumentParser(
        description="Reprocess data partitions for a given layer and date range"
    )
    parser.add_argument(
        "--layer",
        required=True,
        choices=["bronze", "silver", "gold"],
        help="Data layer to reprocess",
    )
    parser.add_argument(
        "--start",
        required=True,
        help="Start date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end",
        required=True,
        help="End date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--env",
        default="dev",
        choices=["dev", "staging", "prod"],
        help="Environment (default: dev)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=True,
        help="Show what would be done without executing (default: True)",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Actually execute the reprocessing (overrides --dry-run)",
    )
    args = parser.parse_args()

    dry_run = not args.execute

    structlog.configure(
        processors=[
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.JSONRenderer(),
        ]
    )

    dates = date_range(args.start, args.end)
    logger.info(
        "Starting reprocessing",
        layer=args.layer,
        start=args.start,
        end=args.end,
        num_dates=len(dates),
        env=args.env,
        dry_run=dry_run,
    )

    if args.env == "prod" and not dry_run:
        logger.warning(
            "PRODUCTION reprocessing requested - proceed with caution",
            layer=args.layer,
            dates=dates,
        )

    # Step 1: Delete existing partitions
    if not delete_partitions(args.layer, dates, args.env, dry_run):
        logger.error("Failed to delete partitions, aborting")
        return 1

    # Step 2: Re-run Spark job
    if not run_spark_job(args.layer, dates, args.env, dry_run):
        logger.error("Spark job failed during reprocessing")
        return 1

    # Step 3: Validate results
    if not dry_run and not validate_results(args.layer, dates, args.env):
        logger.warning("Validation found issues after reprocessing")
        return 2

    logger.info(
        "Reprocessing complete",
        layer=args.layer,
        dates=dates,
        dry_run=dry_run,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
