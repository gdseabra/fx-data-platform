"""ml/feature_store/scripts/materialize_features.py

Materialises features from the Feast offline store into the online store.

Usage:
    python -m ml.feature_store.scripts.materialize_features \\
        --feature-store ml/feature_store/feature_repo/feature_store.yaml \\
        --feature-views user_transaction_features currency_features \\
        --start-date 2024-01-01 \\
        --end-date 2024-01-07

Can also be called from Airflow as a PythonOperator:
    from ml.feature_store.scripts.materialize_features import run_materialization
    run_materialization(start_date, end_date, feature_view_names)
"""

from __future__ import annotations

import argparse
import logging
import os
from datetime import datetime, timezone

from feast import FeatureStore

logger = logging.getLogger(__name__)

_DEFAULT_REPO_PATH = os.environ.get(
    "FEAST_REPO_PATH",
    "ml/feature_store/feature_repo",
)


def run_materialization(
    start_date: datetime,
    end_date: datetime,
    feature_view_names: list[str] | None = None,
    repo_path: str = _DEFAULT_REPO_PATH,
) -> dict:
    """Materialise features from offline → online store.

    Args:
        start_date: Start of the materialisation window (UTC).
        end_date:   End of the materialisation window (UTC).
        feature_view_names: Names to materialise. None → all views.
        repo_path:  Path to the Feast feature_store.yaml directory.

    Returns:
        Dictionary with status and materialised feature view names.
    """
    # Ensure timezone-aware datetimes
    if start_date.tzinfo is None:
        start_date = start_date.replace(tzinfo=timezone.utc)
    if end_date.tzinfo is None:
        end_date = end_date.replace(tzinfo=timezone.utc)

    logger.info("Connecting to Feast store at %s", repo_path)
    store = FeatureStore(repo_path=repo_path)

    if feature_view_names:
        logger.info("Materialising views: %s  [%s → %s]", feature_view_names, start_date, end_date)
        store.materialize(
            start_date=start_date,
            end_date=end_date,
            feature_views=feature_view_names,
        )
        materialised = feature_view_names
    else:
        logger.info("Materialising ALL feature views  [%s → %s]", start_date, end_date)
        store.materialize_incremental(end_date=end_date)
        materialised = [fv.name for fv in store.list_feature_views()]

    logger.info("Materialisation complete. Views: %s", materialised)
    return {"status": "success", "materialised_views": materialised}


def main() -> None:
    parser = argparse.ArgumentParser(description="Materialise Feast features to online store")
    parser.add_argument(
        "--feature-store",
        default=_DEFAULT_REPO_PATH,
        help="Path to the Feast feature_store.yaml directory",
    )
    parser.add_argument(
        "--feature-views",
        nargs="*",
        help="Feature view names to materialise (default: all)",
    )
    parser.add_argument(
        "--start-date",
        required=True,
        help="Start date ISO-8601 (e.g. 2024-01-01)",
    )
    parser.add_argument(
        "--end-date",
        required=True,
        help="End date ISO-8601 (e.g. 2024-01-07)",
    )
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(level=args.log_level, format="%(asctime)s %(levelname)s %(message)s")

    start = datetime.fromisoformat(args.start_date).replace(tzinfo=timezone.utc)
    end = datetime.fromisoformat(args.end_date).replace(tzinfo=timezone.utc)

    result = run_materialization(
        start_date=start,
        end_date=end,
        feature_view_names=args.feature_views or None,
        repo_path=args.feature_store,
    )
    print(result)


if __name__ == "__main__":
    main()
