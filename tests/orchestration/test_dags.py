# DAG validation tests — run without a live Airflow cluster.
# Uses DagBag to import all DAGs and validates structure.
# Run: pytest tests/orchestration/ -m unit -v

import pytest
from pathlib import Path

DAG_DIR = str(Path(__file__).parents[2] / "orchestration" / "dags")

# Skip the entire module if Airflow cannot be imported (e.g. missing system libs).
try:
    import airflow  # noqa: F401
    _AIRFLOW_AVAILABLE = True
except Exception:
    _AIRFLOW_AVAILABLE = False

pytestmark = pytest.mark.skipif(
    not _AIRFLOW_AVAILABLE,
    reason="airflow not importable in this environment (system library mismatch)",
)


@pytest.fixture(scope="module")
def dagbag():
    """Load all DAGs from the orchestration/dags directory."""
    from airflow.models import DagBag
    db = DagBag(dag_folder=DAG_DIR, include_examples=False)
    return db


# ── import health ─────────────────────────────────────────────────────────────

@pytest.mark.unit
def test_no_import_errors(dagbag):
    """All DAG files must be importable without errors."""
    errors = dagbag.import_errors
    assert errors == {}, f"DAG import errors: {errors}"


@pytest.mark.unit
def test_all_expected_dags_present(dagbag):
    """DagBag must contain all three Phase 3 DAGs."""
    expected = {"dag_daily_etl", "dag_streaming_monitor", "dag_ml_pipeline"}
    loaded = set(dagbag.dag_ids)
    missing = expected - loaded
    assert not missing, f"Missing DAGs: {missing}"


# ── dag_daily_etl ─────────────────────────────────────────────────────────────

@pytest.mark.unit
class TestDailyEtlDag:

    @pytest.fixture
    def dag(self, dagbag):
        return dagbag.dags["dag_daily_etl"]

    def test_dag_exists(self, dag):
        assert dag is not None

    def test_schedule(self, dag):
        assert dag.schedule == "0 6 * * *"

    def test_catchup_disabled(self, dag):
        assert dag.catchup is False

    def test_tags(self, dag):
        assert "etl" in dag.tags
        assert "daily" in dag.tags

    def test_all_eight_tasks_present(self, dag):
        task_ids = {t.task_id for t in dag.tasks}
        expected = {
            "catalog_setup",
            "sensor_check_raw_data",
            "bronze_ingest",
            "quality_check_bronze",
            "silver_transform",
            "quality_check_silver",
            "gold_aggregate",
            "iceberg_maintenance",
            "notify_success",
        }
        assert expected == task_ids, f"Task mismatch: {task_ids ^ expected}"

    def test_dependency_chain(self, dag):
        """Verify the linear dependency chain is correctly wired."""
        chain = [
            ("catalog_setup", "sensor_check_raw_data"),
            ("sensor_check_raw_data", "bronze_ingest"),
            ("bronze_ingest", "quality_check_bronze"),
            ("quality_check_bronze", "silver_transform"),
            ("silver_transform", "quality_check_silver"),
            ("quality_check_silver", "gold_aggregate"),
            ("gold_aggregate", "iceberg_maintenance"),
            ("iceberg_maintenance", "notify_success"),
        ]
        task_map = {t.task_id: t for t in dag.tasks}
        for upstream_id, downstream_id in chain:
            upstream = task_map[upstream_id]
            downstream_ids = {t.task_id for t in upstream.downstream_list}
            assert downstream_id in downstream_ids, (
                f"Missing edge: {upstream_id} >> {downstream_id}"
            )

    def test_retries_configured(self, dag):
        for task in dag.tasks:
            assert task.retries >= 1, f"{task.task_id} has no retries"

    def test_sla_miss_callback_set(self, dag):
        assert dag.sla_miss_callback is not None


# ── dag_streaming_monitor ──────────────────────────────────────────────────────

@pytest.mark.unit
class TestStreamingMonitorDag:

    @pytest.fixture
    def dag(self, dagbag):
        return dagbag.dags["dag_streaming_monitor"]

    def test_dag_exists(self, dag):
        assert dag is not None

    def test_schedule_every_15_min(self, dag):
        assert dag.schedule == "*/15 * * * *"

    def test_max_active_runs_is_one(self, dag):
        assert dag.max_active_runs == 1

    def test_expected_tasks_present(self, dag):
        task_ids = {t.task_id for t in dag.tasks}
        for expected_id in (
            "check_redpanda_lag",
            "check_connector_status",
            "check_data_freshness",
            "alert_if_stale",
        ):
            assert expected_id in task_ids

    def test_lag_and_connector_run_before_freshness(self, dag):
        task_map = {t.task_id: t for t in dag.tasks}
        freshness_upstream = {t.task_id for t in task_map["check_data_freshness"].upstream_list}
        assert "check_redpanda_lag" in freshness_upstream
        assert "check_connector_status" in freshness_upstream


# ── dag_ml_pipeline ────────────────────────────────────────────────────────────

@pytest.mark.unit
class TestMlPipelineDag:

    @pytest.fixture
    def dag(self, dagbag):
        return dagbag.dags["dag_ml_pipeline"]

    def test_dag_exists(self, dag):
        assert dag is not None

    def test_weekly_schedule(self, dag):
        assert dag.schedule == "0 8 * * 1"

    def test_linear_ml_pipeline(self, dag):
        task_ids = {t.task_id for t in dag.tasks}
        for expected_id in ("feature_engineering", "training", "validation", "deployment"):
            assert expected_id in task_ids

    def test_dependency_chain(self, dag):
        task_map = {t.task_id: t for t in dag.tasks}
        chain = [
            ("feature_engineering", "training"),
            ("training", "validation"),
            ("validation", "deployment"),
        ]
        for upstream_id, downstream_id in chain:
            downstream_ids = {t.task_id for t in task_map[upstream_id].downstream_list}
            assert downstream_id in downstream_ids
