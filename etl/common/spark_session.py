# FX Data Platform - Spark Session Factory
# Creates and configures SparkSession with Iceberg, S3/MinIO, and sensible defaults.
#
# Catalog strategy:
#   dev  → hadoop catalog (no Hive metastore required, warehouse on MinIO)
#   prod → glue catalog (AWS Glue Data Catalog as Hive-compatible metastore)

import logging
import os

from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

# Iceberg Spark runtime JAR (PySpark 3.5 / Scala 2.12)
ICEBERG_JAR = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3"

# S3A connector JARs needed for MinIO access
HADOOP_AWS_JAR = "org.apache.hadoop:hadoop-aws:3.3.4"
AWS_SDK_JAR = "com.amazonaws:aws-java-sdk-bundle:1.12.262"


def _minio_endpoint() -> str:
    """Resolve MinIO endpoint — supports Docker service name or override via env."""
    return os.environ.get("MINIO_ENDPOINT", "http://minio:9000")


def create_spark_session(
    app_name: str = "fx-data-platform",
    env: str = "dev",
    enable_iceberg: bool = True,
    enable_s3: bool = True,
) -> SparkSession:
    """Create a fully configured SparkSession for the FX data platform.

    Args:
        app_name: Spark application name.
        env:      Runtime environment — drives catalog type and S3 endpoint.
        enable_iceberg: Wire up Iceberg catalog extensions.
        enable_s3:      Configure S3A file system (MinIO for dev, IAM for prod).

    Returns:
        A ready-to-use SparkSession.
    """
    logger.info(f"Creating SparkSession: app={app_name} env={env}")

    builder = (
        SparkSession.builder
        .appName(app_name)
        # Parallelism — keep small for local dev, Spark adapts on a real cluster
        .config("spark.sql.shuffle.partitions", "4" if env == "dev" else "200")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        # Required JARs (downloaded from Maven on first run)
        .config("spark.jars.packages", f"{ICEBERG_JAR},{HADOOP_AWS_JAR},{AWS_SDK_JAR}")
    )

    if enable_iceberg:
        catalog_type = "glue" if env == "prod" else "hadoop"

        builder = (
            builder
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            )
            .config("spark.sql.catalog.bronze", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.bronze.type", catalog_type)
            .config("spark.sql.catalog.bronze.warehouse", _warehouse_path(env, "bronze"))
            .config("spark.sql.catalog.silver", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.silver.type", catalog_type)
            .config("spark.sql.catalog.silver.warehouse", _warehouse_path(env, "silver"))
            .config("spark.sql.catalog.gold", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.gold.type", catalog_type)
            .config("spark.sql.catalog.gold.warehouse", _warehouse_path(env, "gold"))
        )

        if env == "prod":
            # Glue catalog region — read from env in production
            region = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
            for cat in ["bronze", "silver", "gold"]:
                builder = builder.config(f"spark.sql.catalog.{cat}.glue.region", region)

        logger.info(f"Iceberg catalogs: type={catalog_type} bronze/silver/gold → separate buckets")

    if enable_s3:
        if env == "dev":
            endpoint = _minio_endpoint()
            access_key = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
            secret_key = os.environ.get("MINIO_SECRET_KEY", "minioadmin")

            builder = (
                builder
                .config("spark.hadoop.fs.s3a.endpoint", endpoint)
                .config("spark.hadoop.fs.s3a.access.key", access_key)
                .config("spark.hadoop.fs.s3a.secret.key", secret_key)
                .config("spark.hadoop.fs.s3a.path.style.access", "true")
                .config(
                    "spark.hadoop.fs.s3a.impl",
                    "org.apache.hadoop.fs.s3a.S3AFileSystem",
                )
                .config(
                    "spark.hadoop.fs.s3a.aws.credentials.provider",
                    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
                )
            )
            logger.info(f"S3A configured for MinIO at {endpoint}")
        else:
            # In prod, use IAM role credentials (EC2/EMR instance profile)
            builder = builder.config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.InstanceProfileCredentialsProvider",
            )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession created successfully")
    return spark


def _warehouse_path(env: str, layer: str = "bronze") -> str:
    if env == "dev":
        return f"s3a://fx-datalake-{layer}"
    elif env == "staging":
        return f"s3://fx-datalake-staging-{layer}"
    else:
        return f"s3://fx-datalake-prod-{layer}"


def get_spark_session() -> SparkSession:
    """Return the active SparkSession or create a default dev one."""
    active = SparkSession.getActiveSession()
    if active is not None:
        return active
    return create_spark_session()
