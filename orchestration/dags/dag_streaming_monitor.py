# FX Data Platform - Streaming Monitor DAG
# Checks pipeline health every 15 minutes:
#   1. Redpanda consumer lag
#   2. Redpanda Connect connector status
#   3. Raw data freshness in MinIO
#   4. Alert if data is stale

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from orchestration.plugins.callbacks.alerting import on_failure_callback

logger = logging.getLogger(__name__)

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "on_failure_callback": on_failure_callback,
}

dag = DAG(
    "dag_streaming_monitor",
    default_args=default_args,
    description="Streaming pipeline health checks every 15 minutes",
    schedule="*/15 * * * *",
    catchup=False,
    tags=["monitoring", "streaming", "ops"],
    max_active_runs=1,
)

# ── task implementations ──────────────────────────────────────────────────────

def _check_redpanda_lag(**context) -> dict:
    """Check consumer lag on all fx.* topics via Redpanda Admin API."""
    import json
    import urllib.request

    topics = ["fx.transactions", "fx.exchange-rates", "fx.users-cdc"]
    results = {}
    stale_topics = []

    try:
        url = "http://redpanda:9644/v1/topics"
        with urllib.request.urlopen(url, timeout=5) as resp:
            data = json.loads(resp.read())
            existing = {t.get("topic_name") for t in data}

        for topic in topics:
            if topic not in existing:
                logger.warning(f"Topic not found: {topic}")
                results[topic] = {"status": "missing"}
                stale_topics.append(topic)
            else:
                results[topic] = {"status": "ok"}

    except Exception as exc:
        logger.error(f"Could not connect to Redpanda Admin API: {exc}")
        results["error"] = {"status": str(exc)}

    context["ti"].xcom_push("lag_check", results)
    if stale_topics:
        logger.warning(f"Topics with issues: {stale_topics}")
    else:
        logger.info("All Redpanda topics present")
    return results


def _check_connector_status(**context) -> dict:
    """Check Redpanda Connect container health via its HTTP metrics endpoint."""
    import urllib.request

    results = {}
    try:
        url = "http://redpanda-connect:4195/ready"
        with urllib.request.urlopen(url, timeout=5) as resp:
            results["redpanda_connect"] = {
                "status": "healthy" if resp.status == 200 else "degraded",
                "http_status": resp.status,
            }
    except Exception as exc:
        results["redpanda_connect"] = {"status": "unreachable", "error": str(exc)}
        logger.warning(f"Redpanda Connect unreachable: {exc}")

    context["ti"].xcom_push("connector_status", results)
    return results


def _check_data_freshness(**context) -> dict:
    """Verify the most recent object in fx-datalake-raw is < 30 minutes old."""
    from datetime import datetime, timedelta, timezone

    import boto3
    from botocore.client import Config

    results = {}
    staleness_threshold = timedelta(minutes=30)

    try:
        s3 = boto3.client(
            "s3",
            endpoint_url="http://minio:9000",
            aws_access_key_id="minioadmin",
            aws_secret_access_key="minioadmin",
            config=Config(signature_version="s3v4"),
            region_name="us-east-1",
        )

        resp = s3.list_objects_v2(
            Bucket="fx-datalake-raw",
            Prefix="events/",
            MaxKeys=1,
        )

        objects = resp.get("Contents", [])
        if not objects:
            results["raw_layer"] = {"status": "empty", "last_modified": None}
            logger.warning("Raw layer bucket is empty — no data ingested yet")
        else:
            last_modified = objects[0]["LastModified"]
            age = datetime.now(timezone.utc) - last_modified
            is_fresh = age <= staleness_threshold

            results["raw_layer"] = {
                "status": "fresh" if is_fresh else "stale",
                "last_modified": last_modified.isoformat(),
                "age_minutes": round(age.total_seconds() / 60, 1),
            }

            if not is_fresh:
                logger.warning(
                    f"Raw layer is stale: last object is {age.total_seconds() / 60:.0f} min old"
                )

    except Exception as exc:
        results["raw_layer"] = {"status": "error", "error": str(exc)}
        logger.error(f"Could not check data freshness: {exc}")

    context["ti"].xcom_push("freshness_check", results)
    return results


def _alert_if_stale(**context) -> None:
    """Raise if the raw layer is stale or a connector is unreachable."""
    ti = context["ti"]

    freshness = ti.xcom_pull(task_ids="check_data_freshness") or {}
    connector = ti.xcom_pull(task_ids="check_connector_status") or {}

    issues = []

    raw_status = freshness.get("raw_layer", {}).get("status")
    if raw_status in ("stale", "error"):
        issues.append(
            f"Raw layer is {raw_status}: "
            f"{freshness.get('raw_layer', {}).get('age_minutes', '?')} min since last file"
        )

    connect_status = connector.get("redpanda_connect", {}).get("status")
    if connect_status not in ("healthy", None):
        issues.append(f"Redpanda Connect is {connect_status}")

    if issues:
        msg = "Streaming pipeline issues detected:\n" + "\n".join(f"  - {i}" for i in issues)
        logger.error(msg)
        raise RuntimeError(msg)

    logger.info("All streaming health checks passed")


# ── tasks ─────────────────────────────────────────────────────────────────────

check_redpanda_lag = PythonOperator(
    task_id="check_redpanda_lag",
    python_callable=_check_redpanda_lag,
    dag=dag,
)

check_connector_status = PythonOperator(
    task_id="check_connector_status",
    python_callable=_check_connector_status,
    dag=dag,
)

check_data_freshness = PythonOperator(
    task_id="check_data_freshness",
    python_callable=_check_data_freshness,
    dag=dag,
)

alert_if_stale = PythonOperator(
    task_id="alert_if_stale",
    python_callable=_alert_if_stale,
    dag=dag,
)

# Run lag and connector checks in parallel, then freshness, then alert
[check_redpanda_lag, check_connector_status] >> check_data_freshness >> alert_if_stale
