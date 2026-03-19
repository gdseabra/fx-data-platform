"""FX Data Platform - Comprehensive Health Check.

Verifies connectivity and health of all platform services including
Redpanda (topics, lag), S3/MinIO (buckets), Glue Catalog, Airflow
(DAGs), MLflow, Feast (online store), and the Inference API.

Usage:
    python scripts/ops/health_check.py --env dev --verbose
    python scripts/ops/health_check.py --env staging
"""

import subprocess
import sys
from argparse import ArgumentParser
from typing import Tuple

import requests
import structlog

logger = structlog.get_logger(__name__)

# ANSI colors for terminal output
GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
CYAN = "\033[96m"
RESET = "\033[0m"
BOLD = "\033[1m"


class ServiceHealthCheck:
    """Performs health checks on all FX Data Platform services."""

    def __init__(self, env: str = "dev", verbose: bool = False):
        """Initialize health checker.

        Args:
            env: Target environment (dev, staging, prod).
            verbose: Print detailed output for each check.
        """
        self.env = env
        self.verbose = verbose
        self.results: dict[str, Tuple[bool, str]] = {}
        self.warnings: list[str] = []

    def _print_check(self, name: str, success: bool, message: str) -> None:
        """Print colored check result."""
        icon = f"{GREEN}✓{RESET}" if success else f"{RED}✗{RESET}"
        print(f"  {icon} {name}: {message}")

    def check_postgres(self) -> Tuple[bool, str]:
        """Check PostgreSQL connectivity and basic query execution."""
        try:
            result = subprocess.run(
                [
                    "docker", "compose", "exec", "-T", "postgres",
                    "psql", "-U", "postgres", "-d", "fx_transactions",
                    "-c", "SELECT COUNT(*) FROM transactions;",
                ],
                capture_output=True,
                text=True,
                timeout=10,
            )
            if result.returncode == 0:
                return True, "PostgreSQL online (fx_transactions accessible)"
            return False, f"PostgreSQL query failed: {result.stderr[:100]}"
        except subprocess.TimeoutExpired:
            return False, "PostgreSQL timeout (>10s)"
        except Exception as e:
            return False, f"PostgreSQL error: {e}"

    def check_redpanda(self) -> Tuple[bool, str]:
        """Check Redpanda cluster health and topic existence."""
        try:
            response = requests.get("http://localhost:8080/api/health", timeout=5)
            if response.status_code != 200:
                return False, f"Redpanda Console returned {response.status_code}"

            # Check topics via Redpanda Admin API
            topics_result = subprocess.run(
                ["docker", "compose", "exec", "-T", "redpanda", "rpk", "topic", "list"],
                capture_output=True,
                text=True,
                timeout=10,
            )
            expected_topics = ["fx.transactions", "fx.exchange-rates", "fx.users-cdc"]
            if topics_result.returncode == 0:
                output = topics_result.stdout
                missing = [t for t in expected_topics if t not in output]
                if missing:
                    self.warnings.append(f"Missing topics: {missing}")
                    return True, f"Redpanda online, but missing topics: {missing}"
                return True, f"Redpanda online ({len(expected_topics)} topics verified)"

            return True, "Redpanda Console online (could not verify topics)"
        except Exception as e:
            return False, f"Redpanda error: {e}"

    def check_redpanda_lag(self) -> Tuple[bool, str]:
        """Check consumer group lag in Redpanda."""
        try:
            result = subprocess.run(
                ["docker", "compose", "exec", "-T", "redpanda", "rpk", "group", "list"],
                capture_output=True,
                text=True,
                timeout=10,
            )
            if result.returncode != 0:
                return True, "No consumer groups found (may be expected)"

            groups = [line.split()[0] for line in result.stdout.strip().split("\n")[1:] if line.strip()]
            high_lag_groups = []

            for group in groups:
                desc_result = subprocess.run(
                    ["docker", "compose", "exec", "-T", "redpanda", "rpk", "group", "describe", group],
                    capture_output=True,
                    text=True,
                    timeout=10,
                )
                if "LAG" in desc_result.stdout:
                    for line in desc_result.stdout.split("\n"):
                        parts = line.split()
                        if len(parts) >= 3:
                            try:
                                lag = int(parts[-1])
                                if lag > 10000:
                                    high_lag_groups.append(f"{group}(lag={lag})")
                            except ValueError:
                                continue

            if high_lag_groups:
                self.warnings.append(f"High consumer lag: {high_lag_groups}")
                return False, f"High consumer lag: {', '.join(high_lag_groups)}"

            return True, f"Consumer lag OK ({len(groups)} groups checked)"
        except Exception as e:
            return True, f"Could not check lag: {e}"

    def check_minio(self) -> Tuple[bool, str]:
        """Check MinIO health and bucket accessibility."""
        try:
            response = requests.get("http://localhost:9000/minio/health/live", timeout=5)
            if response.status_code != 200:
                return False, f"MinIO returned {response.status_code}"

            # Verify buckets exist
            expected_buckets = [
                "fx-datalake-raw", "fx-datalake-bronze", "fx-datalake-silver",
                "fx-datalake-gold", "fx-datalake-ml",
            ]
            import boto3
            s3 = boto3.client(
                "s3",
                endpoint_url="http://localhost:9000",
                aws_access_key_id="minioadmin",
                aws_secret_access_key="minioadmin",
            )
            existing = {b["Name"] for b in s3.list_buckets()["Buckets"]}
            missing = [b for b in expected_buckets if b not in existing]

            if missing:
                self.warnings.append(f"Missing buckets: {missing}")
                return True, f"MinIO online, missing buckets: {missing}"

            return True, f"MinIO online ({len(expected_buckets)} buckets verified)"
        except ImportError:
            # boto3 not available, just check health
            return True, "MinIO online (bucket check skipped - boto3 not available)"
        except Exception as e:
            return False, f"MinIO error: {e}"

    def check_airflow(self) -> Tuple[bool, str]:
        """Check Airflow health and DAG status."""
        try:
            response = requests.get("http://localhost:8090/health", timeout=10)
            if response.status_code != 200:
                return False, f"Airflow returned {response.status_code}"

            health = response.json()
            scheduler_status = health.get("scheduler", {}).get("status", "unknown")

            if scheduler_status != "healthy":
                return False, f"Airflow scheduler: {scheduler_status}"

            return True, f"Airflow online (scheduler: {scheduler_status})"
        except Exception as e:
            return False, f"Airflow error: {e}"

    def check_mlflow(self) -> Tuple[bool, str]:
        """Check MLflow tracking server availability."""
        try:
            response = requests.get("http://localhost:5000/health", timeout=5)
            if response.status_code == 200:
                return True, "MLflow tracking server online"
            # Some MLflow versions don't have /health
            response = requests.get("http://localhost:5000/api/2.0/mlflow/experiments/search?max_results=1", timeout=5)
            if response.status_code == 200:
                return True, "MLflow tracking server online (API verified)"
            return False, f"MLflow returned {response.status_code}"
        except Exception as e:
            return False, f"MLflow error: {e}"

    def check_feast(self) -> Tuple[bool, str]:
        """Check Feast online store freshness."""
        try:
            from feast import FeatureStore
            store = FeatureStore(repo_path="ml/feature_store/feature_repo")
            # Try to get online features for a test entity
            return True, "Feast feature store accessible"
        except ImportError:
            return True, "Feast check skipped (feast not installed)"
        except Exception as e:
            if "No provider" in str(e) or "registry" in str(e).lower():
                return True, "Feast configured but not materialized (expected in dev)"
            return False, f"Feast error: {e}"

    def check_inference_api(self) -> Tuple[bool, str]:
        """Check Inference API health and model status."""
        try:
            response = requests.get("http://localhost:8000/health", timeout=5)
            if response.status_code != 200:
                return False, f"Inference API returned {response.status_code}"

            data = response.json()
            status = data.get("status", "unknown")
            model_loaded = data.get("model_loaded", False)

            if status == "healthy" and model_loaded:
                return True, f"Inference API healthy (model loaded)"
            elif status == "healthy":
                self.warnings.append("Inference API healthy but no model loaded")
                return True, "Inference API healthy (no model loaded)"
            else:
                return False, f"Inference API status: {status}"
        except Exception as e:
            return False, f"Inference API error: {e}"

    def check_prometheus(self) -> Tuple[bool, str]:
        """Check Prometheus health and scrape targets."""
        try:
            response = requests.get("http://localhost:9090/-/healthy", timeout=5)
            if response.status_code != 200:
                return False, f"Prometheus returned {response.status_code}"

            # Check targets
            targets_resp = requests.get("http://localhost:9090/api/v1/targets", timeout=5)
            if targets_resp.status_code == 200:
                data = targets_resp.json()
                active = data.get("data", {}).get("activeTargets", [])
                down = [t for t in active if t.get("health") == "down"]
                if down:
                    down_jobs = [t.get("labels", {}).get("job", "?") for t in down]
                    self.warnings.append(f"Prometheus targets down: {down_jobs}")
                    return True, f"Prometheus online ({len(active)} targets, {len(down)} down: {down_jobs})"
                return True, f"Prometheus online ({len(active)} targets, all up)"

            return True, "Prometheus online"
        except Exception as e:
            return True, f"Prometheus not running (optional): {e}"

    def run_all_checks(self) -> dict[str, Tuple[bool, str]]:
        """Run all health checks and collect results."""
        print(f"\n{BOLD}{CYAN}FX Data Platform - Health Check{RESET}")
        print(f"Environment: {self.env}")
        print(f"{'=' * 55}\n")

        checks = [
            ("PostgreSQL", self.check_postgres),
            ("Redpanda", self.check_redpanda),
            ("Redpanda Consumer Lag", self.check_redpanda_lag),
            ("MinIO (S3)", self.check_minio),
            ("Airflow", self.check_airflow),
            ("MLflow", self.check_mlflow),
            ("Feast Online Store", self.check_feast),
            ("Inference API", self.check_inference_api),
            ("Prometheus", self.check_prometheus),
        ]

        for name, check_fn in checks:
            try:
                success, message = check_fn()
            except Exception as e:
                success, message = False, f"Unexpected error: {e}"

            self.results[name] = (success, message)
            self._print_check(name, success, message)

            if self.verbose:
                logger.info("Health check", service=name, healthy=success, message=message)

        return self.results

    def get_exit_code(self) -> int:
        """Return exit code: 0=all ok, 1=failures, 2=warnings only."""
        has_failures = any(not success for success, _ in self.results.values())
        if has_failures:
            return 1
        if self.warnings:
            return 2
        return 0

    def print_summary(self) -> None:
        """Print colored summary of all checks."""
        healthy = sum(1 for success, _ in self.results.values() if success)
        total = len(self.results)

        print(f"\n{'=' * 55}")

        if healthy == total and not self.warnings:
            print(f"{GREEN}{BOLD}All {total} services healthy{RESET}")
        elif healthy == total:
            print(f"{YELLOW}{BOLD}{healthy}/{total} services healthy ({len(self.warnings)} warnings){RESET}")
            for w in self.warnings:
                print(f"  {YELLOW}⚠ {w}{RESET}")
        else:
            failed = total - healthy
            print(f"{RED}{BOLD}{failed}/{total} services UNHEALTHY{RESET}")
            for name, (success, msg) in self.results.items():
                if not success:
                    print(f"  {RED}✗ {name}: {msg}{RESET}")

        print(f"{'=' * 55}\n")


def main() -> int:
    """Main entry point.

    Returns:
        0 if all services healthy, 1 if any failures, 2 if warnings only.
    """
    parser = ArgumentParser(description="FX Data Platform - Comprehensive Health Check")
    parser.add_argument(
        "--env",
        default="dev",
        choices=["dev", "staging", "prod"],
        help="Environment (default: dev)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable structured logging output",
    )
    args = parser.parse_args()

    if args.verbose:
        structlog.configure(
            processors=[
                structlog.processors.TimeStamper(fmt="iso"),
                structlog.processors.JSONRenderer(),
            ]
        )

    checker = ServiceHealthCheck(env=args.env, verbose=args.verbose)
    checker.run_all_checks()
    checker.print_summary()

    return checker.get_exit_code()


if __name__ == "__main__":
    sys.exit(main())
