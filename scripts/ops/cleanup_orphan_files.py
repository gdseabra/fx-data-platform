"""FX Data Platform - Cleanup Orphan Files.

Lists and optionally deletes files in S3/MinIO that are not referenced
by any Iceberg table metadata. Helps reclaim storage from failed writes,
expired snapshots, and orphaned data files.

Usage:
    python scripts/ops/cleanup_orphan_files.py --env dev --dry-run
    python scripts/ops/cleanup_orphan_files.py --env dev --execute
    python scripts/ops/cleanup_orphan_files.py --env dev --bucket fx-datalake-bronze --execute
"""

import sys
from argparse import ArgumentParser
from datetime import datetime

import boto3
import structlog

logger = structlog.get_logger(__name__)

BUCKETS = [
    "fx-datalake-raw",
    "fx-datalake-bronze",
    "fx-datalake-silver",
    "fx-datalake-gold",
    "fx-datalake-ml",
]

ENV_CONFIG = {
    "dev": {
        "endpoint_url": "http://localhost:9000",
        "aws_access_key_id": "minioadmin",
        "aws_secret_access_key": "minioadmin",
        "region_name": "us-east-1",
    },
    "staging": {
        "region_name": "us-east-1",
    },
    "prod": {
        "region_name": "us-east-1",
    },
}


def get_s3_client(env: str):
    """Create S3/MinIO client for the given environment."""
    config = ENV_CONFIG.get(env, ENV_CONFIG["dev"])
    return boto3.client("s3", **config)


def get_iceberg_referenced_files(spark, table_name: str) -> set[str]:
    """Get all files referenced by an Iceberg table's current metadata."""
    referenced = set()
    try:
        files_df = spark.sql(f"SELECT file_path FROM {table_name}.files")
        for row in files_df.collect():
            referenced.add(row["file_path"])

        manifest_df = spark.sql(f"SELECT path FROM {table_name}.manifests")
        for row in manifest_df.collect():
            referenced.add(row["path"])
    except Exception as e:
        logger.warning("Could not read Iceberg metadata", table=table_name, error=str(e))

    return referenced


def list_s3_files(s3_client, bucket: str) -> list[dict]:
    """List all files in an S3 bucket with size info."""
    files = []
    paginator = s3_client.get_paginator("list_objects_v2")

    try:
        for page in paginator.paginate(Bucket=bucket):
            for obj in page.get("Contents", []):
                files.append({
                    "key": obj["Key"],
                    "size": obj["Size"],
                    "last_modified": obj["LastModified"].isoformat(),
                    "full_path": f"s3a://{bucket}/{obj['Key']}",
                })
    except Exception as e:
        logger.error("Failed to list bucket", bucket=bucket, error=str(e))

    return files


def find_orphans(s3_files: list[dict], referenced_files: set[str]) -> list[dict]:
    """Find files that exist in S3 but are not referenced by Iceberg."""
    orphans = []
    for f in s3_files:
        if f["full_path"] not in referenced_files and f["key"] not in referenced_files:
            # Skip metadata files that are always needed
            if any(f["key"].endswith(ext) for ext in [".metadata.json", "version-hint.text"]):
                continue
            orphans.append(f)
    return orphans


def delete_orphans(s3_client, bucket: str, orphans: list[dict]) -> int:
    """Delete orphan files from S3. Returns count of deleted files."""
    deleted = 0
    # Delete in batches of 1000 (S3 limit)
    batch_size = 1000
    for i in range(0, len(orphans), batch_size):
        batch = orphans[i:i + batch_size]
        objects = [{"Key": f["key"]} for f in batch]

        try:
            response = s3_client.delete_objects(
                Bucket=bucket,
                Delete={"Objects": objects, "Quiet": True},
            )
            errors = response.get("Errors", [])
            deleted += len(batch) - len(errors)

            if errors:
                for err in errors:
                    logger.error("Delete failed", key=err["Key"], code=err["Code"])
        except Exception as e:
            logger.error("Batch delete failed", bucket=bucket, error=str(e))

    return deleted


def format_size(size_bytes: int) -> str:
    """Format bytes as human-readable string."""
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size_bytes < 1024:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.1f} PB"


def main() -> int:
    """Main entry point.

    Returns:
        0 if successful, 1 on error, 2 on warning.
    """
    parser = ArgumentParser(
        description="Find and remove orphan files not referenced by Iceberg tables"
    )
    parser.add_argument(
        "--env",
        default="dev",
        choices=["dev", "staging", "prod"],
        help="Environment (default: dev)",
    )
    parser.add_argument(
        "--bucket",
        help="Specific bucket to clean (default: all datalake buckets)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=True,
        help="Show what would be deleted without executing (default: True)",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Actually delete orphan files (overrides --dry-run)",
    )
    args = parser.parse_args()

    dry_run = not args.execute

    structlog.configure(
        processors=[
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.JSONRenderer(),
        ]
    )

    buckets = [args.bucket] if args.bucket else BUCKETS
    s3_client = get_s3_client(args.env)

    # Optionally get Iceberg-referenced files
    referenced_files = set()
    try:
        from etl.common.spark_session import create_spark_session
        spark = create_spark_session(env=args.env, app_name="cleanup-orphans")

        for table in ["bronze.transactions", "silver.transactions", "gold.daily_volume",
                       "gold.user_summary", "gold.hourly_rates", "gold.anomaly_summary"]:
            refs = get_iceberg_referenced_files(spark, table)
            referenced_files.update(refs)
            logger.info("Loaded Iceberg refs", table=table, file_count=len(refs))

        spark.stop()
    except ImportError:
        logger.warning(
            "Could not load Spark — will list all files without Iceberg cross-reference. "
            "Orphan detection will be less accurate."
        )

    total_orphans = 0
    total_space = 0
    total_deleted = 0

    for bucket in buckets:
        logger.info("Scanning bucket", bucket=bucket)
        s3_files = list_s3_files(s3_client, bucket)

        if referenced_files:
            orphans = find_orphans(s3_files, referenced_files)
        else:
            # Without Iceberg refs, just report all files
            orphans = []
            logger.info(
                "Listing all files (no Iceberg cross-reference)",
                bucket=bucket,
                file_count=len(s3_files),
                total_size=format_size(sum(f["size"] for f in s3_files)),
            )
            continue

        orphan_size = sum(f["size"] for f in orphans)
        total_orphans += len(orphans)
        total_space += orphan_size

        logger.info(
            "Orphan scan complete",
            bucket=bucket,
            total_files=len(s3_files),
            orphan_files=len(orphans),
            orphan_size=format_size(orphan_size),
        )

        if orphans and not dry_run:
            if args.env == "prod":
                logger.warning("Deleting orphans in PRODUCTION", bucket=bucket, count=len(orphans))

            deleted = delete_orphans(s3_client, bucket, orphans)
            total_deleted += deleted
            logger.info("Deleted orphans", bucket=bucket, deleted=deleted)
        elif orphans and dry_run:
            for f in orphans[:20]:
                logger.info("DRY RUN - would delete", key=f["key"], size=format_size(f["size"]))
            if len(orphans) > 20:
                logger.info(f"... and {len(orphans) - 20} more files")

    # Summary
    print("\n" + "=" * 60)
    print(f"Orphan Files Summary")
    print(f"  Total orphans found: {total_orphans}")
    print(f"  Total space to reclaim: {format_size(total_space)}")
    if not dry_run:
        print(f"  Total files deleted: {total_deleted}")
    else:
        print(f"  Mode: DRY RUN (use --execute to delete)")
    print("=" * 60)

    return 0


if __name__ == "__main__":
    sys.exit(main())
