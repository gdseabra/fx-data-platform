# Conftest for orchestration tests.
# Initialises a temporary Airflow SQLite database so that DagBag can be
# instantiated without a running Airflow cluster.

import os
import subprocess
import tempfile
import pytest


@pytest.fixture(scope="session", autouse=True)
def airflow_db(tmp_path_factory):
    """Create a throw-away Airflow home with an initialised SQLite database."""
    airflow_home = str(tmp_path_factory.mktemp("airflow_home"))
    env = {**os.environ, "AIRFLOW_HOME": airflow_home}
    subprocess.run(
        ["airflow", "db", "migrate"],
        env=env,
        check=True,
        capture_output=True,
    )
    os.environ["AIRFLOW_HOME"] = airflow_home
    yield
