# FX Data Platform - SparkIcebergOperator
# Wraps SparkSubmitOperator with Iceberg and S3/MinIO configs pre-applied.
# All ETL DAGs should use this instead of the raw SparkSubmitOperator.

from __future__ import annotations

import os
from typing import Any

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# Iceberg Spark runtime JAR (Spark 3.5 / Scala 2.12)
_ICEBERG_JAR = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3"
_HADOOP_AWS = "org.apache.hadoop:hadoop-aws:3.3.4"
_AWS_SDK = "com.amazonaws:aws-java-sdk-bundle:1.12.262"
_PACKAGES = f"{_ICEBERG_JAR},{_HADOOP_AWS},{_AWS_SDK}"

# Default Iceberg + S3A conf for dev/local (MinIO)
_DEFAULT_CONF: dict[str, str] = {
    "spark.sql.extensions": (
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    ),
    "spark.sql.catalog.bronze": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.bronze.type": "hadoop",
    "spark.sql.catalog.bronze.warehouse": "s3a://fx-datalake-bronze",
    "spark.sql.catalog.silver": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.silver.type": "hadoop",
    "spark.sql.catalog.silver.warehouse": "s3a://fx-datalake-silver",
    "spark.sql.catalog.gold": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.gold.type": "hadoop",
    "spark.sql.catalog.gold.warehouse": "s3a://fx-datalake-gold",
    "spark.hadoop.fs.s3a.endpoint": os.environ.get("MINIO_ENDPOINT", "http://minio:9000"),
    "spark.hadoop.fs.s3a.access.key": os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
    "spark.hadoop.fs.s3a.secret.key": os.environ.get("MINIO_SECRET_KEY", "minioadmin"),
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.sql.shuffle.partitions": "4",
    "spark.sql.adaptive.enabled": "true",
}


class SparkIcebergOperator(SparkSubmitOperator):
    """SparkSubmitOperator with Iceberg + S3 configs pre-wired.

    Usage in a DAG:

        bronze_ingest = SparkIcebergOperator(
            task_id="bronze_ingest",
            application="/workspace/etl/jobs/bronze/ingest_transactions.py",
            application_args=["--env", "dev", "--date", "{{ ds }}"],
            driver_memory="2g",
            executor_memory="2g",
        )

    Extra ``conf`` values passed at call site are *merged* on top of the
    defaults — caller values win on conflict.
    """

    def __init__(
        self,
        *,
        conf: dict[str, str] | None = None,
        packages: str | None = None,
        conn_id: str = "spark_local",
        **kwargs: Any,
    ) -> None:
        merged_conf = {**_DEFAULT_CONF, **(conf or {})}
        merged_packages = f"{_PACKAGES},{packages}" if packages else _PACKAGES

        super().__init__(
            conn_id=conn_id,
            conf=merged_conf,
            packages=merged_packages,
            **kwargs,
        )
