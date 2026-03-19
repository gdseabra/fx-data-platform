"""ml/serving/scripts/load_test.py

Locust load test for the FX Anomaly Detection inference service.

Simulates 100 requests/second against POST /predict.
Validates:
  - Error rate < 0.1%
  - p95 latency < 100ms

Usage:
    # Run interactively (web UI on http://localhost:8089):
    locust -f ml/serving/scripts/load_test.py --host http://localhost:8000

    # Run headless (CI):
    locust -f ml/serving/scripts/load_test.py \\
        --host http://localhost:8000 \\
        --headless \\
        --users 20 \\
        --spawn-rate 5 \\
        --run-time 60s \\
        --html reports/load_test_report.html \\
        --exit-code-on-error 1
"""

from __future__ import annotations

import random
import uuid
from datetime import datetime, timezone

from locust import HttpUser, between, events, task
from locust.runners import MasterRunner


# ---------------------------------------------------------------------------
# Transaction factory
# ---------------------------------------------------------------------------

_CURRENCIES = ["USD", "EUR", "GBP", "JPY", "CHF", "AUD", "CAD", "MXN"]
_ANOMALY_RATE = 0.05  # 5% of requests send an anomalous transaction


def _make_transaction(anomalous: bool = False) -> dict:
    if anomalous:
        amount_brl = random.uniform(50_000, 200_000)
        spread_pct = random.uniform(0.10, 0.30)
    else:
        amount_brl = random.expovariate(1 / 2_000) + 100
        spread_pct = random.uniform(0.005, 0.03)

    return {
        "transaction_id": str(uuid.uuid4()),
        "user_id": str(uuid.uuid4()),
        "amount_brl": round(amount_brl, 2),
        "currency": random.choice(_CURRENCIES),
        "timestamp": datetime.now(tz=timezone.utc).isoformat(),
        "spread_pct": round(spread_pct, 4),
    }


# ---------------------------------------------------------------------------
# Locust user
# ---------------------------------------------------------------------------

class InferenceUser(HttpUser):
    """Simulates a single API consumer hitting /predict."""

    # Wait 10–50ms between requests → ~20–100 req/s per user
    wait_time = between(0.01, 0.05)

    @task(19)
    def predict_normal(self):
        """Normal transaction — 95% of requests."""
        payload = _make_transaction(anomalous=False)
        with self.client.post(
            "/predict",
            json=payload,
            name="/predict [normal]",
            catch_response=True,
        ) as resp:
            if resp.status_code == 200:
                data = resp.json()
                if data.get("risk_level") == "high":
                    resp.failure(f"High risk for normal transaction: {data}")
                else:
                    resp.success()
            else:
                resp.failure(f"HTTP {resp.status_code}: {resp.text[:200]}")

    @task(1)
    def predict_anomalous(self):
        """Anomalous transaction — 5% of requests."""
        payload = _make_transaction(anomalous=True)
        with self.client.post(
            "/predict",
            json=payload,
            name="/predict [anomalous]",
            catch_response=True,
        ) as resp:
            if resp.status_code == 200:
                resp.success()
            else:
                resp.failure(f"HTTP {resp.status_code}: {resp.text[:200]}")

    @task
    def health_check(self):
        """Periodic health check — included in overall request mix."""
        with self.client.get("/health", name="/health", catch_response=True) as resp:
            if resp.status_code == 200:
                data = resp.json()
                if data.get("model_loaded"):
                    resp.success()
                else:
                    resp.failure("Model not loaded")
            else:
                resp.failure(f"HTTP {resp.status_code}")


# ---------------------------------------------------------------------------
# Validation thresholds (checked at test end)
# ---------------------------------------------------------------------------

_MAX_ERROR_RATE_PCT = 0.1   # 0.1%
_MAX_P95_LATENCY_MS = 100   # 100ms


@events.quitting.add_listener
def _check_thresholds(environment, **kwargs):
    """Fail the test if SLOs are breached."""
    stats = environment.runner.stats.total

    error_rate = (stats.num_failures / max(stats.num_requests, 1)) * 100
    p95 = stats.get_response_time_percentile(0.95)

    violations = []
    if error_rate > _MAX_ERROR_RATE_PCT:
        violations.append(f"Error rate {error_rate:.2f}% > {_MAX_ERROR_RATE_PCT}%")
    if p95 and p95 > _MAX_P95_LATENCY_MS:
        violations.append(f"p95 latency {p95:.0f}ms > {_MAX_P95_LATENCY_MS}ms")

    if violations:
        print("\n[LOAD TEST FAILED]")
        for v in violations:
            print(f"  ✗ {v}")
        environment.process_exit_code = 1
    else:
        print(
            f"\n[LOAD TEST PASSED]  "
            f"error_rate={error_rate:.3f}%  p95={p95:.0f}ms  "
            f"requests={stats.num_requests}"
        )
