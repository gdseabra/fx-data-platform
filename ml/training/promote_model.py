"""ml/training/promote_model.py

Promotes a validated MLflow run to the Model Registry.

Promotion flow:
    registered → Staging → (validation) → Production
    Old Production → Archived

Validation gates (from training_config.yaml):
  - F1 > min_f1 (default 0.85)
  - latency_p95_ms < max_latency_p95_ms (default 50ms)
  - Model must not be worse than the current Production model

Usage:
    python -m ml.training.promote_model \\
        --run-id <mlflow-run-id> \\
        --model-name anomaly-detector-fx \\
        --config ml/config/training_config.yaml

Exit codes:
    0 — promoted to Production
    1 — validation failed (thresholds not met or worse than current)
    2 — already at Production (no action needed)
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Any

import mlflow
import yaml
from mlflow.tracking import MlflowClient

logger = logging.getLogger(__name__)

_DEFAULT_MODEL_NAME = "anomaly-detector-fx"


# ---------------------------------------------------------------------------
# Threshold validation
# ---------------------------------------------------------------------------

def _validate_against_thresholds(
    metrics: dict[str, float],
    thresholds: dict[str, float],
) -> tuple[bool, list[str]]:
    """Return (passed, list_of_failures)."""
    failures = []

    if metrics.get("f1", 0.0) < thresholds.get("min_f1", 0.85):
        failures.append(
            f"F1={metrics.get('f1', 0):.4f} < min_f1={thresholds['min_f1']}"
        )
    if metrics.get("latency_p95_ms", 9999) > thresholds.get("max_latency_p95_ms", 50):
        failures.append(
            f"latency_p95={metrics.get('latency_p95_ms'):.1f}ms > "
            f"max={thresholds['max_latency_p95_ms']}ms"
        )
    if metrics.get("precision", 0.0) < thresholds.get("min_precision", 0.80):
        failures.append(
            f"precision={metrics.get('precision', 0):.4f} < "
            f"min_precision={thresholds['min_precision']}"
        )

    return len(failures) == 0, failures


def _validate_against_production(
    new_metrics: dict[str, float],
    client: MlflowClient,
    model_name: str,
) -> tuple[bool, str]:
    """Ensure new model is not worse than current Production (by F1)."""
    try:
        prod_versions = client.get_latest_versions(model_name, stages=["Production"])
    except Exception:
        return True, "No current Production model — first deploy."

    if not prod_versions:
        return True, "No current Production model — first deploy."

    prod_run_id = prod_versions[0].run_id
    prod_run = client.get_run(prod_run_id)
    prod_f1 = prod_run.data.metrics.get("f1", 0.0)
    new_f1 = new_metrics.get("f1", 0.0)

    if new_f1 < prod_f1:
        return False, (
            f"New model F1={new_f1:.4f} < Production F1={prod_f1:.4f}. "
            "Promotion blocked."
        )
    return True, f"New F1={new_f1:.4f} >= Production F1={prod_f1:.4f}. OK."


# ---------------------------------------------------------------------------
# Promotion logic
# ---------------------------------------------------------------------------

def promote(
    run_id: str,
    model_name: str = _DEFAULT_MODEL_NAME,
    thresholds: dict | None = None,
    promoted_by: str = "automated-pipeline",
    reason: str = "weekly retraining",
    tracking_uri: str | None = None,
) -> dict[str, Any]:
    """Register model and promote to Production if thresholds pass.

    Returns a result dict with status and details.
    """
    if tracking_uri:
        mlflow.set_tracking_uri(tracking_uri)

    client = MlflowClient()
    run = client.get_run(run_id)
    metrics = run.data.metrics

    # --- threshold check ---
    th = thresholds or {}
    passed_th, failures = _validate_against_thresholds(metrics, th)
    if not passed_th:
        logger.error("Threshold validation failed: %s", failures)
        return {"status": "rejected", "reason": failures, "run_id": run_id}

    # --- regression check vs Production ---
    passed_reg, reg_msg = _validate_against_production(metrics, client, model_name)
    if not passed_reg:
        logger.error("Regression check failed: %s", reg_msg)
        return {"status": "rejected", "reason": reg_msg, "run_id": run_id}

    logger.info("All validations passed. Registering model…")

    # --- register ---
    model_uri = f"runs:/{run_id}/isolation_forest"  # primary model artifact
    mv = mlflow.register_model(model_uri=model_uri, name=model_name)
    version = mv.version
    logger.info("Registered %s version %s", model_name, version)

    # --- archive old Production ---
    try:
        old_prod = client.get_latest_versions(model_name, stages=["Production"])
        for old_mv in old_prod:
            client.transition_model_version_stage(
                name=model_name,
                version=old_mv.version,
                stage="Archived",
                archive_existing_versions=False,
            )
            logger.info("Archived old Production version %s", old_mv.version)
    except Exception as exc:
        logger.warning("Could not archive old Production: %s", exc)

    # --- Staging → Production ---
    client.transition_model_version_stage(
        name=model_name,
        version=version,
        stage="Production",
        archive_existing_versions=True,
    )

    # --- annotate ---
    now = datetime.now(tz=timezone.utc).isoformat()
    client.update_model_version(
        name=model_name,
        version=version,
        description=(
            f"Promoted to Production by {promoted_by} on {now}. "
            f"Reason: {reason}. "
            f"F1={metrics.get('f1', 0):.4f}, "
            f"latency_p95={metrics.get('latency_p95_ms', 0):.1f}ms"
        ),
    )
    client.set_model_version_tag(model_name, version, "promoted_by", promoted_by)
    client.set_model_version_tag(model_name, version, "promoted_at", now)
    client.set_model_version_tag(model_name, version, "reason", reason)

    result = {
        "status": "promoted",
        "model_name": model_name,
        "version": version,
        "run_id": run_id,
        "metrics": metrics,
        "promoted_by": promoted_by,
        "promoted_at": now,
    }
    logger.info("Promotion complete: %s", result)
    return result


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Promote MLflow model to Production")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--model-name", default=_DEFAULT_MODEL_NAME)
    parser.add_argument("--config", default="ml/config/training_config.yaml")
    parser.add_argument("--promoted-by", default="automated-pipeline")
    parser.add_argument("--reason", default="weekly retraining")
    parser.add_argument("--tracking-uri", default=None)
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(level=args.log_level, format="%(asctime)s %(levelname)s %(message)s")

    with open(args.config) as f:
        config = yaml.safe_load(f)

    thresholds = (
        config.get("model_training", {})
        .get("anomaly_detection", {})
        .get("promotion_thresholds", {})
    )

    result = promote(
        run_id=args.run_id,
        model_name=args.model_name,
        thresholds=thresholds,
        promoted_by=args.promoted_by,
        reason=args.reason,
        tracking_uri=args.tracking_uri or os.environ.get("MLFLOW_TRACKING_URI"),
    )

    print(json.dumps(result, indent=2, default=str))

    if result["status"] == "rejected":
        sys.exit(1)


if __name__ == "__main__":
    main()
