"""ml/training/evaluate_model.py

Loads the N most recent MLflow runs from the anomaly-detection experiment,
compares metrics side-by-side, and recommends the best model based on a
composite score of F1 + inference latency.

Produces:
  - Terminal table with all run metrics
  - HTML report with charts (precision-recall, score distributions, latency box)
  - Returns the winning run_id (used by promote_model.py)

Usage:
    python -m ml.training.evaluate_model \\
        --experiment fx-anomaly-detection \\
        --n-runs 10 \\
        --output reports/evaluation.html
"""

from __future__ import annotations

import argparse
import json
import logging
import os
from pathlib import Path
from typing import Any

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import mlflow
import pandas as pd
from mlflow.tracking import MlflowClient

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load_runs(
    experiment_name: str,
    n_runs: int,
    model_type_filter: str | None = None,
) -> pd.DataFrame:
    """Fetch the N most recent finished runs from MLflow."""
    client = MlflowClient()
    experiment = client.get_experiment_by_name(experiment_name)
    if experiment is None:
        raise ValueError(f"Experiment '{experiment_name}' not found.")

    runs = client.search_runs(
        experiment_ids=[experiment.experiment_id],
        filter_string=(
            f"tags.model_type = '{model_type_filter}'" if model_type_filter else ""
        ),
        order_by=["start_time DESC"],
        max_results=n_runs,
    )

    rows = []
    for run in runs:
        row: dict[str, Any] = {
            "run_id": run.info.run_id,
            "run_name": run.info.run_name,
            "start_time": pd.Timestamp(run.info.start_time, unit="ms"),
            "status": run.info.status,
        }
        row.update(run.data.metrics)
        row.update({f"param.{k}": v for k, v in run.data.params.items()})
        row.update({f"tag.{k}": v for k, v in run.data.tags.items()})
        rows.append(row)

    return pd.DataFrame(rows)


def _composite_score(row: pd.Series, latency_weight: float = 0.2) -> float:
    """Weighted composite: F1 * (1 - latency_weight) + latency_score * latency_weight.

    latency_score = 1 if p95 < 10ms, linearly degrades to 0 at 100ms.
    """
    f1 = row.get("f1", 0.0) or 0.0
    p95 = row.get("latency_p95_ms", 50.0) or 50.0
    latency_score = max(0.0, 1.0 - (p95 / 100.0))
    return f1 * (1 - latency_weight) + latency_score * latency_weight


# ---------------------------------------------------------------------------
# HTML report
# ---------------------------------------------------------------------------

def _build_html_report(df: pd.DataFrame, winner_run_id: str, output_path: Path) -> None:
    """Generate a self-contained HTML report with embedded charts."""
    import base64
    import io

    sections = []

    # 1. Metrics table
    metric_cols = [c for c in df.columns if not c.startswith("param.") and not c.startswith("tag.")]
    table_html = df[metric_cols].to_html(
        index=False, classes="metrics-table", border=0, float_format="%.4f"
    )
    sections.append(f"<h2>Run Comparison</h2>{table_html}")

    # 2. F1 bar chart
    fig, ax = plt.subplots(figsize=(10, 4))
    labels = df["run_id"].str[:8].tolist()
    values = df.get("f1", pd.Series([0.0] * len(df))).fillna(0).tolist()
    colors = ["#2ecc71" if rid == winner_run_id else "#3498db" for rid in df["run_id"]]
    ax.bar(labels, values, color=colors)
    ax.set_ylim(0, 1)
    ax.set_xlabel("Run ID (first 8 chars)")
    ax.set_ylabel("F1 Score")
    ax.set_title("F1 Score per Run  (green = recommended)")
    buf = io.BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight")
    buf.seek(0)
    f1_chart = base64.b64encode(buf.read()).decode()
    plt.close(fig)
    sections.append(
        f'<h2>F1 Score Comparison</h2>'
        f'<img src="data:image/png;base64,{f1_chart}" style="max-width:100%"/>'
    )

    # 3. Latency chart
    if "latency_p95_ms" in df.columns:
        fig, ax = plt.subplots(figsize=(10, 4))
        lat_values = df["latency_p95_ms"].fillna(0).tolist()
        ax.bar(labels, lat_values, color=colors)
        ax.axhline(50, color="red", linestyle="--", label="50ms threshold")
        ax.set_xlabel("Run ID (first 8 chars)")
        ax.set_ylabel("p95 Latency (ms)")
        ax.set_title("p95 Inference Latency per Run")
        ax.legend()
        buf = io.BytesIO()
        fig.savefig(buf, format="png", bbox_inches="tight")
        buf.seek(0)
        lat_chart = base64.b64encode(buf.read()).decode()
        plt.close(fig)
        sections.append(
            f'<h2>p95 Latency Comparison</h2>'
            f'<img src="data:image/png;base64,{lat_chart}" style="max-width:100%"/>'
        )

    html = f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8"/>
<title>FX Anomaly Detection — Model Evaluation Report</title>
<style>
  body {{ font-family: Arial, sans-serif; margin: 40px; color: #333; }}
  h1 {{ color: #2c3e50; }}
  h2 {{ color: #2980b9; border-bottom: 1px solid #bdc3c7; padding-bottom: 4px; }}
  .metrics-table {{ border-collapse: collapse; width: 100%; font-size: 13px; }}
  .metrics-table th, .metrics-table td {{ border: 1px solid #ddd; padding: 6px 10px; text-align: right; }}
  .metrics-table th {{ background: #2980b9; color: white; }}
  .winner {{ background: #d5f5e3; font-weight: bold; }}
</style>
</head>
<body>
<h1>FX Anomaly Detection — Model Evaluation Report</h1>
<p><strong>Recommended run:</strong> <code>{winner_run_id}</code></p>
{"".join(sections)}
</body>
</html>"""

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(html)
    logger.info("HTML report written to %s", output_path)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run_evaluation(
    experiment_name: str = "fx-anomaly-detection",
    n_runs: int = 10,
    output_path: str = "reports/evaluation.html",
    tracking_uri: str | None = None,
) -> str:
    """Compare runs and return the winning run_id."""
    if tracking_uri:
        mlflow.set_tracking_uri(tracking_uri)

    df = _load_runs(experiment_name, n_runs)
    if df.empty:
        raise ValueError("No finished runs found. Run train_anomaly_detector.py first.")

    df["composite_score"] = df.apply(_composite_score, axis=1)
    df = df.sort_values("composite_score", ascending=False).reset_index(drop=True)

    winner = df.iloc[0]
    winner_run_id: str = winner["run_id"]

    print("\n=== Top runs by composite score (F1 + latency) ===")
    display_cols = [c for c in ["run_id", "run_name", "f1", "precision", "recall", "auc_roc",
                                 "latency_p95_ms", "composite_score"] if c in df.columns]
    print(df[display_cols].to_string(index=False))
    print(f"\n✓ Recommended run: {winner_run_id}  (composite={winner['composite_score']:.4f})")

    _build_html_report(df, winner_run_id, Path(output_path))
    return winner_run_id


def main() -> None:
    parser = argparse.ArgumentParser(description="Evaluate and compare MLflow training runs")
    parser.add_argument("--experiment", default="fx-anomaly-detection")
    parser.add_argument("--n-runs", type=int, default=10)
    parser.add_argument("--output", default="reports/evaluation.html")
    parser.add_argument("--tracking-uri", default=None)
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(level=args.log_level, format="%(asctime)s %(levelname)s %(message)s")

    winner_run_id = run_evaluation(
        experiment_name=args.experiment,
        n_runs=args.n_runs,
        output_path=args.output,
        tracking_uri=args.tracking_uri,
    )
    print(json.dumps({"winner_run_id": winner_run_id}))


if __name__ == "__main__":
    main()
