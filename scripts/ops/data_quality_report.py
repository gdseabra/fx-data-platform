"""FX Data Platform - Data Quality Report Generator.

Connects to Iceberg tables and generates statistics including row counts,
null percentages, value distributions, and freshness per table. Outputs
an HTML report with charts.

Usage:
    python scripts/ops/data_quality_report.py --env dev
    python scripts/ops/data_quality_report.py --env dev --output report.html --days 30
    python scripts/ops/data_quality_report.py --env dev --dry-run
"""

import json
import sys
from argparse import ArgumentParser
from datetime import datetime, timedelta
from pathlib import Path

import structlog

logger = structlog.get_logger(__name__)

TABLES = {
    "bronze.transactions": {
        "layer": "bronze",
        "partition_column": "event_date",
        "key_columns": ["transaction_id"],
        "numeric_columns": ["amount_brl", "amount_foreign", "exchange_rate", "spread_pct", "fee_brl"],
        "categorical_columns": ["currency", "transaction_type", "status", "channel"],
    },
    "silver.transactions": {
        "layer": "silver",
        "partition_column": "event_date",
        "key_columns": ["transaction_id"],
        "numeric_columns": ["amount_brl", "amount_foreign", "exchange_rate", "spread_pct", "amount_usd"],
        "categorical_columns": ["currency", "transaction_type", "status", "channel"],
    },
    "gold.daily_volume": {
        "layer": "gold",
        "partition_column": "event_date",
        "key_columns": ["event_date", "currency"],
        "numeric_columns": ["total_volume", "transaction_count", "avg_amount", "p95_amount"],
        "categorical_columns": ["currency"],
    },
    "gold.user_summary": {
        "layer": "gold",
        "partition_column": None,
        "key_columns": ["user_id"],
        "numeric_columns": ["total_transactions", "total_volume", "avg_ticket"],
        "categorical_columns": ["tier"],
    },
}


def get_spark_session(env: str):
    """Create SparkSession for querying Iceberg tables."""
    try:
        from etl.common.spark_session import create_spark_session
        return create_spark_session(env=env, app_name="data-quality-report")
    except ImportError:
        logger.warning("Could not import spark_session, returning None")
        return None


def compute_table_stats(spark, table_name: str, table_config: dict, days: int) -> dict:
    """Compute quality statistics for a single table."""
    stats = {
        "table": table_name,
        "layer": table_config["layer"],
        "timestamp": datetime.utcnow().isoformat(),
    }

    try:
        df = spark.table(table_name)
        total_rows = df.count()
        stats["total_rows"] = total_rows

        if total_rows == 0:
            stats["status"] = "empty"
            return stats

        # Null percentages per column
        null_stats = {}
        for col_name in df.columns:
            null_count = df.where(df[col_name].isNull()).count()
            null_stats[col_name] = {
                "null_count": null_count,
                "null_pct": round(null_count / total_rows * 100, 2),
            }
        stats["null_analysis"] = null_stats

        # Numeric column distributions
        numeric_stats = {}
        for col_name in table_config.get("numeric_columns", []):
            summary = df.select(col_name).summary("min", "25%", "50%", "75%", "max", "mean", "stddev").collect()
            numeric_stats[col_name] = {row["summary"]: row[col_name] for row in summary}
        stats["numeric_distributions"] = numeric_stats

        # Row counts by partition (last N days)
        partition_col = table_config.get("partition_column")
        if partition_col:
            cutoff = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")
            daily_counts = (
                df.where(f"{partition_col} >= '{cutoff}'")
                .groupBy(partition_col)
                .count()
                .orderBy(partition_col)
                .collect()
            )
            stats["daily_row_counts"] = [
                {"date": str(row[partition_col]), "count": row["count"]}
                for row in daily_counts
            ]

        # Freshness: max timestamp in table
        if partition_col:
            max_date = df.agg({partition_col: "max"}).collect()[0][0]
            stats["latest_partition"] = str(max_date) if max_date else None
            if max_date:
                freshness_hours = (datetime.utcnow() - datetime.strptime(str(max_date), "%Y-%m-%d")).total_seconds() / 3600
                stats["freshness_hours"] = round(freshness_hours, 1)

        stats["status"] = "ok"

    except Exception as e:
        logger.error("Failed to compute stats", table=table_name, error=str(e))
        stats["status"] = "error"
        stats["error"] = str(e)

    return stats


def generate_html_report(all_stats: list[dict], output_path: str) -> None:
    """Generate an HTML report with quality statistics."""
    html_parts = [
        "<!DOCTYPE html>",
        "<html><head>",
        "<title>FX Data Platform - Data Quality Report</title>",
        "<style>",
        "  body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; margin: 20px; background: #f5f5f5; }",
        "  h1 { color: #1a1a2e; }",
        "  h2 { color: #16213e; border-bottom: 2px solid #0f3460; padding-bottom: 5px; }",
        "  .card { background: white; border-radius: 8px; padding: 20px; margin: 15px 0; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }",
        "  table { border-collapse: collapse; width: 100%; margin: 10px 0; }",
        "  th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }",
        "  th { background: #0f3460; color: white; }",
        "  tr:nth-child(even) { background: #f2f2f2; }",
        "  .status-ok { color: #27ae60; font-weight: bold; }",
        "  .status-error { color: #e74c3c; font-weight: bold; }",
        "  .status-empty { color: #f39c12; font-weight: bold; }",
        "  .metric { display: inline-block; margin: 10px; padding: 15px; background: #e8f4f8; border-radius: 8px; min-width: 150px; text-align: center; }",
        "  .metric .value { font-size: 24px; font-weight: bold; color: #0f3460; }",
        "  .metric .label { font-size: 12px; color: #666; }",
        "  .warn { background: #fff3cd; }",
        "</style>",
        "</head><body>",
        "<h1>FX Data Platform - Data Quality Report</h1>",
        f"<p>Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}</p>",
    ]

    # Summary metrics
    total_tables = len(all_stats)
    ok_tables = sum(1 for s in all_stats if s.get("status") == "ok")
    total_rows = sum(s.get("total_rows", 0) for s in all_stats)

    html_parts.append('<div class="card">')
    html_parts.append("<h2>Summary</h2>")
    html_parts.append(f'<div class="metric"><div class="value">{ok_tables}/{total_tables}</div><div class="label">Tables Healthy</div></div>')
    html_parts.append(f'<div class="metric"><div class="value">{total_rows:,}</div><div class="label">Total Rows</div></div>')
    html_parts.append("</div>")

    # Per-table details
    for stats in all_stats:
        table_name = stats["table"]
        status_class = f"status-{stats.get('status', 'error')}"

        html_parts.append('<div class="card">')
        html_parts.append(f'<h2>{table_name} <span class="{status_class}">[{stats.get("status", "unknown")}]</span></h2>')

        if stats.get("total_rows") is not None:
            html_parts.append(f'<div class="metric"><div class="value">{stats["total_rows"]:,}</div><div class="label">Total Rows</div></div>')

        if stats.get("freshness_hours") is not None:
            warn_class = ' warn' if stats["freshness_hours"] > 24 else ''
            html_parts.append(f'<div class="metric{warn_class}"><div class="value">{stats["freshness_hours"]}h</div><div class="label">Freshness</div></div>')

        if stats.get("latest_partition"):
            html_parts.append(f'<div class="metric"><div class="value">{stats["latest_partition"]}</div><div class="label">Latest Partition</div></div>')

        # Null analysis table
        if stats.get("null_analysis"):
            html_parts.append("<h3>Null Analysis</h3>")
            html_parts.append("<table><tr><th>Column</th><th>Null Count</th><th>Null %</th></tr>")
            for col, info in stats["null_analysis"].items():
                warn = ' class="warn"' if info["null_pct"] > 5 else ""
                html_parts.append(f'<tr{warn}><td>{col}</td><td>{info["null_count"]:,}</td><td>{info["null_pct"]}%</td></tr>')
            html_parts.append("</table>")

        # Numeric distributions
        if stats.get("numeric_distributions"):
            html_parts.append("<h3>Numeric Distributions</h3>")
            html_parts.append("<table><tr><th>Column</th><th>Min</th><th>25%</th><th>Median</th><th>75%</th><th>Max</th><th>Mean</th><th>StdDev</th></tr>")
            for col, dist in stats["numeric_distributions"].items():
                html_parts.append(
                    f'<tr><td>{col}</td>'
                    f'<td>{dist.get("min", "N/A")}</td>'
                    f'<td>{dist.get("25%", "N/A")}</td>'
                    f'<td>{dist.get("50%", "N/A")}</td>'
                    f'<td>{dist.get("75%", "N/A")}</td>'
                    f'<td>{dist.get("max", "N/A")}</td>'
                    f'<td>{dist.get("mean", "N/A")}</td>'
                    f'<td>{dist.get("stddev", "N/A")}</td></tr>'
                )
            html_parts.append("</table>")

        # Daily row counts
        if stats.get("daily_row_counts"):
            html_parts.append("<h3>Daily Row Counts (last N days)</h3>")
            html_parts.append("<table><tr><th>Date</th><th>Count</th></tr>")
            for entry in stats["daily_row_counts"][-30:]:
                html_parts.append(f'<tr><td>{entry["date"]}</td><td>{entry["count"]:,}</td></tr>')
            html_parts.append("</table>")

        if stats.get("error"):
            html_parts.append(f'<p class="status-error">Error: {stats["error"]}</p>')

        html_parts.append("</div>")

    html_parts.append("</body></html>")

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    Path(output_path).write_text("\n".join(html_parts))
    logger.info("HTML report generated", path=output_path)


def main() -> int:
    """Main entry point.

    Returns:
        0 if successful, 1 on error, 2 on warning.

    """
    parser = ArgumentParser(
        description="Generate data quality report for all Iceberg tables"
    )
    parser.add_argument(
        "--env",
        default="dev",
        choices=["dev", "staging", "prod"],
        help="Environment (default: dev)",
    )
    parser.add_argument(
        "--output",
        default="docs/data_quality_report.html",
        help="Output HTML report path (default: docs/data_quality_report.html)",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=30,
        help="Number of days to analyze (default: 30)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without executing",
    )
    args = parser.parse_args()

    structlog.configure(
        processors=[
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.JSONRenderer(),
        ]
    )

    logger.info(
        "Starting data quality report",
        env=args.env,
        days=args.days,
        output=args.output,
        dry_run=args.dry_run,
    )

    if args.dry_run:
        logger.info("DRY RUN - would analyze tables", tables=list(TABLES.keys()))
        return 0

    spark = get_spark_session(args.env)
    if spark is None:
        logger.error("Could not create SparkSession")
        return 1

    all_stats = []
    has_warnings = False

    try:
        for table_name, table_config in TABLES.items():
            logger.info("Analyzing table", table=table_name)
            stats = compute_table_stats(spark, table_name, table_config, args.days)
            all_stats.append(stats)

            if stats.get("status") == "error":
                has_warnings = True
            elif stats.get("freshness_hours", 0) > 48:
                has_warnings = True
                logger.warning("Table is stale", table=table_name, freshness_hours=stats["freshness_hours"])

        generate_html_report(all_stats, args.output)

        # Also save raw JSON for programmatic access
        json_path = args.output.replace(".html", ".json")
        Path(json_path).write_text(json.dumps(all_stats, indent=2, default=str))
        logger.info("JSON report saved", path=json_path)

    finally:
        spark.stop()

    if has_warnings:
        logger.warning("Report generated with warnings")
        return 2

    logger.info("Report generated successfully")
    return 0


if __name__ == "__main__":
    sys.exit(main())
