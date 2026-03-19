# FX Data Platform - Airflow Alerting Callbacks
# Reusable on_failure / on_sla_miss / on_success callbacks for DAGs and tasks.
# Currently logs structured JSON; swap _send() for Slack/PagerDuty in production.

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)


# ── internal sender (plug in Slack / email / PagerDuty here) ─────────────────

def _send(level: str, title: str, body: dict[str, Any]) -> None:
    """Emit a structured alert. Replace with real notification in production."""
    payload = {
        "level": level,
        "title": title,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        **body,
    }
    log_fn = logger.error if level == "CRITICAL" else logger.warning
    log_fn("ALERT: %s", json.dumps(payload))

    # TODO (production): send to Slack / PagerDuty
    # import requests
    # requests.post(SLACK_WEBHOOK, json={"text": f"*{title}*\n```{json.dumps(body, indent=2)}```"})


# ── callbacks ─────────────────────────────────────────────────────────────────

def on_failure_callback(context: dict[str, Any]) -> None:
    """Called by Airflow when any task fails.

    Attach to individual tasks or set as dag-level default_args callback.

    Example:
        default_args = {"on_failure_callback": on_failure_callback}
    """
    ti = context.get("task_instance")
    exception = context.get("exception")

    _send(
        level="CRITICAL",
        title=f"Task FAILED: {ti.dag_id}.{ti.task_id}",
        body={
            "dag_id": ti.dag_id,
            "task_id": ti.task_id,
            "run_id": ti.run_id,
            "execution_date": str(context.get("execution_date")),
            "try_number": ti.try_number,
            "exception": str(exception) if exception else None,
            "log_url": ti.log_url,
        },
    )


def on_sla_miss_callback(
    dag: Any,
    task_list: str,
    blocking_task_list: str,
    slas: Any,
    blocking_tis: Any,
) -> None:
    """Called by Airflow when a task misses its SLA.

    Attach at DAG level:
        dag = DAG(..., sla_miss_callback=on_sla_miss_callback)
    """
    _send(
        level="WARNING",
        title=f"SLA missed: {dag.dag_id}",
        body={
            "dag_id": dag.dag_id,
            "tasks_missed_sla": task_list,
            "blocking_tasks": blocking_task_list,
        },
    )


def on_success_callback(context: dict[str, Any]) -> None:
    """Called when a task succeeds (use sparingly — high volume).

    Typically attached only to the final task of a DAG to signal completion.

    Example:
        notify = PythonOperator(
            task_id="notify_success",
            python_callable=lambda **ctx: on_success_callback(ctx),
        )
    """
    ti = context.get("task_instance")

    _send(
        level="INFO",
        title=f"DAG completed: {ti.dag_id}",
        body={
            "dag_id": ti.dag_id,
            "run_id": ti.run_id,
            "execution_date": str(context.get("execution_date")),
            "duration_seconds": (
                (ti.end_date - ti.start_date).total_seconds()
                if ti.end_date and ti.start_date
                else None
            ),
        },
    )
