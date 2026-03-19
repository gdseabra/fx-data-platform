# FX Data Platform - Iceberg Time Travel Examples
#
# Demonstrates three Iceberg capabilities:
#   1. Query at a specific snapshot (audit / point-in-time analysis)
#   2. Rollback a table to a previous snapshot (incident recovery)
#   3. Incremental read — changes since the last processed snapshot
#
# These examples run in local Spark mode against MinIO.
# Run: python -m etl.iceberg.time_travel_examples

import logging

from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)


# ── 1. Query at a specific snapshot ───────────────────────────────────────────

def query_at_snapshot(
    spark: SparkSession,
    table: str,
    snapshot_id: int,
) -> DataFrame:
    """Return the state of *table* as it was at *snapshot_id*.

    Iceberg keeps all snapshot metadata, so you can query any historical
    version regardless of when data was modified or compacted.

    Why useful: regulatory audits, debugging wrong aggregations, comparing
    model training data at different points in time.

    Example:
        df = query_at_snapshot(spark, "fx_silver.transactions", 5432109876)
        df.show()

    """
    logger.info(f"Querying {table} at snapshot {snapshot_id}")
    return spark.read.option("snapshot-id", snapshot_id).table(table)


def query_at_timestamp(
    spark: SparkSession,
    table: str,
    as_of_timestamp: str,
) -> DataFrame:
    """Return the state of *table* as it was at *as_of_timestamp* (UTC ISO-8601).

    Iceberg finds the snapshot whose committed_at is <= the given timestamp.

    Example:
        df = query_at_timestamp(
            spark, "fx_silver.transactions", "2024-03-15T06:00:00"
        )

    """
    logger.info(f"Querying {table} as of {as_of_timestamp}")
    return (
        spark.read
        .option("as-of-timestamp", _to_millis(as_of_timestamp))
        .table(table)
    )


# ── 2. Rollback ────────────────────────────────────────────────────────────────

def rollback_to_snapshot(
    spark: SparkSession,
    table: str,
    snapshot_id: int,
) -> None:
    """Roll *table* back to *snapshot_id*.

    This is a metadata-only operation — no data files are deleted.
    The current table state is replaced with the state at the target snapshot.

    Why useful: quick recovery when a bad ETL run corrupts a table. The
    erroneous snapshot still exists and can be re-inspected later.

    Example:
        rollback_to_snapshot(spark, "fx_silver.transactions", 5432109876)

    """
    logger.info(f"Rolling back {table} to snapshot {snapshot_id}")
    spark.sql(f"""
        CALL spark_catalog.system.rollback_to_snapshot(
            table       => '{table}',
            snapshot_id => {snapshot_id}
        )
    """)
    logger.info(f"Rollback complete for {table}")


def rollback_to_timestamp(
    spark: SparkSession,
    table: str,
    as_of_timestamp: str,
) -> None:
    """Roll *table* back to the snapshot that was current at *as_of_timestamp*.

    Example:
        rollback_to_timestamp(spark, "fx_silver.transactions", "2024-03-15T00:00:00")

    """
    logger.info(f"Rolling back {table} to state at {as_of_timestamp}")
    spark.sql(f"""
        CALL spark_catalog.system.rollback_to_timestamp(
            table  => '{table}',
            timestamp => TIMESTAMP '{as_of_timestamp}'
        )
    """)
    logger.info(f"Rollback complete for {table}")


# ── 3. Incremental read ────────────────────────────────────────────────────────

def incremental_read(
    spark: SparkSession,
    table: str,
    start_snapshot_id: int,
    end_snapshot_id: int | None = None,
) -> DataFrame:
    """Read only the rows that changed between two snapshots.

    Iceberg computes the diff by comparing manifest files; only the changed
    data files are scanned. This is vastly cheaper than a full-table scan for
    downstream CDC-style pipelines.

    Why useful: feeding incremental ML training, building event streams from
    batch tables, feeding downstream aggregations that only need deltas.

    Args:
        start_snapshot_id: Exclusive lower bound (changes *after* this snapshot).
        end_snapshot_id:   Inclusive upper bound (defaults to latest snapshot).

    Example:
        df = incremental_read(spark, "fx_silver.transactions", 1111, 2222)
        df.show()

    """
    logger.info(
        f"Incremental read on {table}: "
        f"snapshots ({start_snapshot_id}, {end_snapshot_id or 'latest'}]"
    )

    reader = spark.read.option("start-snapshot-id", start_snapshot_id)
    if end_snapshot_id is not None:
        reader = reader.option("end-snapshot-id", end_snapshot_id)

    return reader.format("iceberg").load(table)


# ── snapshot utilities ─────────────────────────────────────────────────────────

def list_snapshots(spark: SparkSession, table: str) -> DataFrame:
    """List all snapshots for *table* with their metadata.

    Returns columns: committed_at, snapshot_id, parent_id, operation, summary.
    """
    return spark.sql(f"SELECT * FROM {table}.snapshots ORDER BY committed_at DESC")


def get_latest_snapshot_id(spark: SparkSession, table: str) -> int:
    """Return the snapshot_id of the most recent committed snapshot."""
    row = (
        spark.sql(
            f"SELECT snapshot_id FROM {table}.snapshots ORDER BY committed_at DESC LIMIT 1"
        )
        .collect()
    )
    if not row:
        raise ValueError(f"No snapshots found for {table}")
    return int(row[0]["snapshot_id"])


# ── helpers ────────────────────────────────────────────────────────────────────

def _to_millis(iso_ts: str) -> int:
    """Convert an ISO-8601 UTC string to epoch milliseconds."""
    from datetime import datetime, timezone
    dt = datetime.fromisoformat(iso_ts).replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


# ── demo ───────────────────────────────────────────────────────────────────────

def run_demo(spark: SparkSession, table: str = "fx_silver.transactions") -> None:
    """Run all three time-travel patterns against *table*."""
    print(f"\n=== Iceberg Time Travel Demo: {table} ===\n")

    # List available snapshots
    print("-- Available snapshots:")
    snapshots = list_snapshots(spark, table)
    snapshots.show(5, truncate=False)

    rows = snapshots.collect()
    if len(rows) < 2:
        print("Need at least 2 snapshots to demo incremental read — run more ETL first.")
        return

    latest_id = int(rows[0]["snapshot_id"])
    prev_id = int(rows[1]["snapshot_id"])

    # 1. Point-in-time query
    print(f"\n-- 1. State at snapshot {prev_id}:")
    query_at_snapshot(spark, table, prev_id).show(3)

    # 2. Incremental read (changes in latest snapshot)
    print(f"\n-- 3. Rows added in snapshot {latest_id} (incremental):")
    incremental_read(spark, table, prev_id, latest_id).show(3)

    print("\nNote: rollback example skipped in demo (destructive operation).")
    print(f"  To rollback: rollback_to_snapshot(spark, '{table}', {prev_id})")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    from etl.common.spark_session import create_spark_session

    spark = create_spark_session("iceberg-time-travel", env="dev")
    try:
        run_demo(spark)
    finally:
        spark.stop()
