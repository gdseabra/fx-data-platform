# orchestration/dags/dag_ml_pipeline.py
# ML pipeline DAG — Phase 4 full implementation.
#
# Schedule: every Monday at 08:00 UTC
# Tasks:
#   feature_engineering → training → validation → deployment
#
# Each task calls the corresponding Phase 4 module:
#   feature_engineering  →  ml.feature_store.scripts.materialize_features
#   training             →  ml.training.train_anomaly_detector
#   validation           →  ml.training.evaluate_model
#   deployment           →  ml.training.promote_model

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone

import yaml
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator

from orchestration.plugins.callbacks.alerting import on_failure_callback

logger = logging.getLogger(__name__)

_CONFIG_PATH = os.environ.get("ML_CONFIG_PATH", "ml/config/training_config.yaml")
_MODEL_NAME = os.environ.get("MODEL_NAME", "anomaly-detector-fx")
_FEAST_REPO = os.environ.get("FEAST_REPO_PATH", "ml/feature_store/feature_repo")


def _load_config() -> dict:
    with open(_CONFIG_PATH) as f:
        return yaml.safe_load(f)


# ── task callables ────────────────────────────────────────────────────────────

def _feature_engineering(**context) -> None:
    """Materialise Feast features from Silver/Gold tables into the online store.

    Window: last 7 days (enough for a weekly pipeline run).
    Pushes feature_dataset_path to XCom for the training task.
    """
    from ml.feature_store.scripts.materialize_features import run_materialization

    execution_date: datetime = context["data_interval_end"].replace(tzinfo=timezone.utc)
    start_date = execution_date - timedelta(days=7)

    logger.info("Materialising features  %s → %s", start_date.date(), execution_date.date())

    result = run_materialization(
        start_date=start_date,
        end_date=execution_date,
        feature_view_names=None,  # materialise all views
        repo_path=_FEAST_REPO,
    )

    if result.get("status") != "success":
        raise AirflowException(f"Feature materialisation failed: {result}")

    logger.info("Materialised views: %s", result["materialised_views"])
    context["ti"].xcom_push(key="feature_dataset_path", value=_FEAST_REPO)


def _training(**context) -> None:
    """Train Isolation Forest and LOF models; track both runs in MLflow.

    Pushes the run_ids dict to XCom for the validation task.
    """
    from ml.training.train_anomaly_detector import run_pipeline

    config = _load_config()
    env = os.environ.get("ENV", "dev")

    logger.info("Starting training pipeline (env=%s)", env)
    results = run_pipeline(config=config, env=env)

    failed = [m for m, r in results.items() if r.get("status") != "success"]
    if failed:
        raise AirflowException(f"Training failed for models: {failed}")

    run_ids = {model: r["run_id"] for model, r in results.items()}
    logger.info("Training complete. run_ids=%s", run_ids)
    context["ti"].xcom_push(key="run_ids", value=run_ids)
    context["ti"].xcom_push(key="training_results", value=results)


def _validation(**context) -> None:
    """Evaluate all trained runs; select the best model by composite score.

    Gates promotion: raises AirflowException if no model meets the thresholds.
    Pushes winner_run_id to XCom.
    """
    from ml.training.evaluate_model import run_evaluation

    config = _load_config()
    experiment_name = config["experiments"]["anomaly_detection"]["name"]
    thresholds = (
        config.get("model_training", {})
        .get("anomaly_detection", {})
        .get("promotion_thresholds", {})
    )

    winner_run_id = run_evaluation(
        experiment_name=experiment_name,
        n_runs=6,  # compare last 6 runs (current + 5 previous)
        output_path="reports/ml_pipeline_evaluation.html",
    )

    # Verify winning run meets minimum thresholds before pushing
    import mlflow
    from mlflow.tracking import MlflowClient

    mlflow.set_tracking_uri(os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow:5000"))
    client = MlflowClient()
    run = client.get_run(winner_run_id)
    metrics = run.data.metrics

    min_f1 = thresholds.get("min_f1", 0.85)
    max_p95 = thresholds.get("max_latency_p95_ms", 50)

    if metrics.get("f1", 0.0) < min_f1:
        raise AirflowException(
            f"Best model F1={metrics.get('f1', 0):.4f} < threshold {min_f1}. "
            "Deployment blocked."
        )
    if metrics.get("latency_p95_ms", 9999) > max_p95:
        raise AirflowException(
            f"Best model p95={metrics.get('latency_p95_ms'):.1f}ms > threshold {max_p95}ms. "
            "Deployment blocked."
        )

    logger.info("Validation passed. Winner run: %s  F1=%.4f", winner_run_id, metrics.get("f1", 0))
    context["ti"].xcom_push(key="winner_run_id", value=winner_run_id)
    context["ti"].xcom_push(key="winner_metrics", value=dict(metrics))


def _deployment(**context) -> None:
    """Promote validated model to Production in the MLflow Model Registry."""
    from ml.training.promote_model import promote

    config = _load_config()
    thresholds = (
        config.get("model_training", {})
        .get("anomaly_detection", {})
        .get("promotion_thresholds", {})
    )

    winner_run_id: str = context["ti"].xcom_pull(task_ids="validation", key="winner_run_id")
    if not winner_run_id:
        raise AirflowException("No winner_run_id from validation task.")

    execution_date = context["data_interval_end"]
    result = promote(
        run_id=winner_run_id,
        model_name=_MODEL_NAME,
        thresholds=thresholds,
        promoted_by="airflow/dag_ml_pipeline",
        reason=f"weekly retraining — execution_date={execution_date.date()}",
        tracking_uri=os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow:5000"),
    )

    if result["status"] == "rejected":
        raise AirflowException(f"Promotion rejected: {result['reason']}")

    logger.info(
        "Model promoted to Production: name=%s version=%s",
        result["model_name"], result["version"],
    )
    context["ti"].xcom_push(key="promoted_version", value=result["version"])
    context["ti"].xcom_push(key="promotion_result", value=result)


# ── DAG definition ────────────────────────────────────────────────────────────

default_args = {
    "owner": "ml-engineering",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "on_failure_callback": on_failure_callback,
}

dag = DAG(
    "dag_ml_pipeline",
    default_args=default_args,
    description="Weekly ML pipeline: feature materialisation → training → evaluation → promotion",
    schedule="0 8 * * 1",  # Every Monday at 08:00 UTC
    catchup=False,
    max_active_runs=1,
    tags=["ml", "weekly", "anomaly-detection"],
)

feature_engineering = PythonOperator(
    task_id="feature_engineering",
    python_callable=_feature_engineering,
    dag=dag,
)

training = PythonOperator(
    task_id="training",
    python_callable=_training,
    execution_timeout=timedelta(hours=2),
    dag=dag,
)

validation = PythonOperator(
    task_id="validation",
    python_callable=_validation,
    dag=dag,
)

deployment = PythonOperator(
    task_id="deployment",
    python_callable=_deployment,
    dag=dag,
)

# ── dependencies ──────────────────────────────────────────────────────────────

feature_engineering >> training >> validation >> deployment
