# FX Data Platform - Daily ETL Pipeline DAG
# Orchestrates: Raw → Bronze → Silver → Gold
# Schedule: daily at 06:00 UTC with 2h SLA
#
# Task order:
#   sensor_check_raw_data
#   → bronze_ingest
#   → quality_check_bronze
#   → silver_transform
#   → quality_check_silver
#   → gold_aggregate
#   → iceberg_maintenance
#   → notify_success

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor

from orchestration.plugins.callbacks.alerting import (
    on_failure_callback,
    on_sla_miss_callback,
)
from orchestration.plugins.operators.spark_iceberg_operator import SparkIcebergOperator

logger = logging.getLogger(__name__)

# ── DAG defaults ──────────────────────────────────────────────────────────────

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "on_failure_callback": on_failure_callback,
}

dag = DAG(
    "dag_daily_etl",
    default_args=default_args,
    description="Daily ETL Pipeline: Raw → Bronze → Silver → Gold",
    schedule="0 6 * * *",
    catchup=False,
    tags=["etl", "daily", "production"],
    sla_miss_callback=on_sla_miss_callback,
)

# ── task helpers ──────────────────────────────────────────────────────────────

def _check_raw_data(**context) -> bool:
    """Return True when the raw partition for {{ ds }} is non-empty in MinIO."""
    import boto3
    from botocore.client import Config

    ds = context["ds"]  # execution date: YYYY-MM-DD
    bucket = "fx-datalake-raw"
    prefix = f"events/event_date={ds}/"

    s3 = boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )

    resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
    found = resp.get("KeyCount", 0) > 0
    if not found:
        logger.info(f"Raw partition not ready yet: s3://{bucket}/{prefix}")
    return found


def _quality_check_bronze(**context) -> None:
    """Run quality checks on fx_bronze.transactions for {{ ds }}."""
    from etl.common.quality import QualityChecks
    from etl.common.spark_session import create_spark_session

    spark = create_spark_session("qc-bronze", env="dev")
    try:
        df = spark.table("bronze.transactions")
        report = QualityChecks.run_all(
            df,
            checks=[
                lambda d: QualityChecks.check_not_null(d, "transaction_id"),
                lambda d: QualityChecks.check_not_null(d, "user_id"),
                lambda d: QualityChecks.check_not_null(d, "timestamp"),
                lambda d: QualityChecks.check_range(d, "amount_brl", 0.01, 10_000_000),
                lambda d: QualityChecks.check_row_count(d, 1, "bronze.transactions"),
            ],
            table="bronze.transactions",
        )
        report.log()
        report.raise_if_failed()
        # Push metrics to XCom for downstream visibility
        context["ti"].xcom_push("bronze_quality", report.summary())
    finally:
        spark.stop()


def _quality_check_silver(**context) -> None:
    """Run quality checks on fx_silver.transactions."""
    from etl.common.quality import QualityChecks
    from etl.common.spark_session import create_spark_session

    spark = create_spark_session("qc-silver", env="dev")
    try:
        df = spark.table("silver.transactions")
        report = QualityChecks.run_all(
            df,
            checks=[
                lambda d: QualityChecks.check_not_null(d, "transaction_id"),
                lambda d: QualityChecks.check_not_null(d, "amount_usd"),
                lambda d: QualityChecks.check_range(d, "amount_brl", 0.01, 10_000_000),
                lambda d: QualityChecks.check_allowed_values(
                    d, "currency",
                    {"USD", "EUR", "GBP", "JPY", "AUD", "CAD", "CNY", "CHF", "MXN", "ARS"},
                ),
                lambda d: QualityChecks.check_row_count(d, 1, "silver.transactions"),
            ],
            table="silver.transactions",
        )
        report.log()
        report.raise_if_failed()
        context["ti"].xcom_push("silver_quality", report.summary())
    finally:
        spark.stop()


def _run_iceberg_maintenance(**context) -> None:
    """Expire snapshots, remove orphan files, compact small files."""
    from etl.common.spark_session import create_spark_session
    from etl.iceberg.maintenance import run_all

    spark = create_spark_session("iceberg-maintenance", env="dev")
    try:
        run_all(spark=spark)
    finally:
        spark.stop()


def _notify_success(**context) -> None:
    from orchestration.plugins.callbacks.alerting import on_success_callback
    on_success_callback(context)


# ── tasks ─────────────────────────────────────────────────────────────────────

# 1. Catalog setup — creates Iceberg namespaces and tables if not yet present (idempotent)
catalog_setup = SparkIcebergOperator(
    task_id="catalog_setup",
    application="/workspace/etl/iceberg/catalog_setup.py",
    application_args=["--env", "dev"],
    driver_memory="1g",
    executor_memory="1g",
    dag=dag,
)

# 2. Sensor: wait for raw partition to land in MinIO
sensor_check_raw_data = PythonSensor(
    task_id="sensor_check_raw_data",
    python_callable=_check_raw_data,
    poke_interval=300,          # check every 5 min
    timeout=3 * 3600,           # give up after 3 hours
    mode="reschedule",          # don't hold a worker slot while poking
    sla=timedelta(hours=1),
    dag=dag,
)

# 3. Bronze ingestion (Raw JSON → Iceberg bronze.transactions)
bronze_ingest = SparkIcebergOperator(
    task_id="bronze_ingest",
    application="/workspace/etl/jobs/bronze/ingest_transactions.py",
    application_args=["--env", "dev", "--date", "{{ ds }}"],
    driver_memory="2g",
    executor_memory="2g",
    sla=timedelta(hours=1),
    dag=dag,
)

# 4. Quality check on Bronze
quality_check_bronze = PythonOperator(
    task_id="quality_check_bronze",
    python_callable=_quality_check_bronze,
    sla=timedelta(minutes=15),
    dag=dag,
)

# 5. Silver transformation (bronze → silver)
silver_transform = SparkIcebergOperator(
    task_id="silver_transform",
    application="/workspace/etl/jobs/silver/transform_transactions.py",
    application_args=["--env", "dev"],
    driver_memory="2g",
    executor_memory="2g",
    sla=timedelta(hours=1),
    dag=dag,
)

# 6. Quality check on Silver
quality_check_silver = PythonOperator(
    task_id="quality_check_silver",
    python_callable=_quality_check_silver,
    sla=timedelta(minutes=15),
    dag=dag,
)

# 7. Gold aggregation (silver → gold tables)
gold_aggregate = SparkIcebergOperator(
    task_id="gold_aggregate",
    application="/workspace/etl/jobs/gold/aggregate_metrics.py",
    application_args=["--env", "dev"],
    driver_memory="2g",
    executor_memory="2g",
    sla=timedelta(hours=1),
    dag=dag,
)

# 8. Iceberg maintenance (snapshot expiration + compaction)
iceberg_maintenance = PythonOperator(
    task_id="iceberg_maintenance",
    python_callable=_run_iceberg_maintenance,
    sla=timedelta(minutes=30),
    dag=dag,
)

# 9. Notify success
notify_success = PythonOperator(
    task_id="notify_success",
    python_callable=_notify_success,
    dag=dag,
)

# ── dependencies ──────────────────────────────────────────────────────────────

(
    catalog_setup
    >> sensor_check_raw_data
    >> bronze_ingest
    >> quality_check_bronze
    >> silver_transform
    >> quality_check_silver
    >> gold_aggregate
    >> iceberg_maintenance
    >> notify_success
)
