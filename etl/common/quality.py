# FX Data Platform - Data Quality Framework
# Reusable quality checks with structured reporting.
# Each check returns a QualityCheck result; a QualityReport aggregates them.

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List

from pyspark.sql import DataFrame
from pyspark.sql.functions import col

logger = logging.getLogger(__name__)


# ── result types ──────────────────────────────────────────────────────────────

@dataclass
class QualityCheck:
    """Result of a single data quality check."""

    name: str
    passed: bool
    details: Dict[str, Any]

    def __str__(self) -> str:
        status = "PASS" if self.passed else "FAIL"
        return f"[{status}] {self.name} -- {self.details}"

    # Keep old __repr__ alias for backwards compat
    __repr__ = __str__


@dataclass
class QualityReport:
    """Aggregated results from multiple quality checks."""

    table: str
    checks: List[QualityCheck] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return all(c.passed for c in self.checks)

    @property
    def failed_checks(self) -> List[QualityCheck]:
        return [c for c in self.checks if not c.passed]

    def summary(self) -> Dict[str, Any]:
        return {
            "table": self.table,
            "total_checks": len(self.checks),
            "passed": sum(1 for c in self.checks if c.passed),
            "failed": len(self.failed_checks),
            "overall": "PASS" if self.passed else "FAIL",
            "failures": [str(c) for c in self.failed_checks],
        }

    def log(self) -> None:
        s = self.summary()
        level = logging.INFO if self.passed else logging.WARNING
        logger.log(level, f"Quality report for {self.table}: {s}")

    def raise_if_failed(self) -> None:
        """Raise ValueError if any check failed (use in Airflow PythonOperator)."""
        if not self.passed:
            failures = "; ".join(str(c) for c in self.failed_checks)
            raise ValueError(f"Quality checks FAILED for {self.table}: {failures}")


# ── individual checks ─────────────────────────────────────────────────────────

class QualityChecks:
    """Static factory methods for common quality checks."""

    @staticmethod
    def check_not_null(df: DataFrame, column: str) -> QualityCheck:
        """Check that no rows have null in *column*."""
        total = df.count()
        null_count = df.filter(col(column).isNull()).count()
        pct = null_count / total * 100 if total > 0 else 0.0
        return QualityCheck(
            name=f"NOT_NULL({column})",
            passed=null_count == 0,
            details={"null_count": null_count, "total_rows": total, "null_pct": round(pct, 2)},
        )

    @staticmethod
    def check_unique(df: DataFrame, column: str) -> QualityCheck:
        """Check that all values in *column* are distinct."""
        total = df.count()
        unique = df.select(column).distinct().count()
        duplicates = total - unique
        return QualityCheck(
            name=f"UNIQUE({column})",
            passed=duplicates == 0,
            details={"total_rows": total, "unique_values": unique, "duplicates": duplicates},
        )

    @staticmethod
    def check_range(
        df: DataFrame,
        column: str,
        min_val: float,
        max_val: float,
    ) -> QualityCheck:
        """Check that all values in *column* lie within [min_val, max_val]."""
        out_of_range = df.filter(
            (col(column) < min_val) | (col(column) > max_val)
        ).count()
        return QualityCheck(
            name=f"RANGE({column}, [{min_val}, {max_val}])",
            passed=out_of_range == 0,
            details={"out_of_range_count": out_of_range, "min": min_val, "max": max_val},
        )

    @staticmethod
    def check_allowed_values(
        df: DataFrame,
        column: str,
        allowed: set,
    ) -> QualityCheck:
        """Check that every value in *column* is in the *allowed* set."""
        invalid = df.filter(~col(column).isin(*allowed)).count()
        return QualityCheck(
            name=f"ALLOWED_VALUES({column})",
            passed=invalid == 0,
            details={"invalid_count": invalid, "allowed_values": sorted(allowed)},
        )

    @staticmethod
    def check_referential_integrity(
        df: DataFrame,
        fk_column: str,
        ref_df: DataFrame,
        pk_column: str,
    ) -> QualityCheck:
        """Check that every value in df[fk_column] exists in ref_df[pk_column].

        Uses a left-anti join to find FK values with no matching PK.

        Example:
            check_referential_integrity(
                transactions_df, "user_id",
                users_df,        "user_id",
            )
        """
        orphans = (
            df.select(fk_column)
            .distinct()
            .join(
                ref_df.select(col(pk_column).alias(fk_column)),
                on=fk_column,
                how="left_anti",
            )
            .count()
        )
        return QualityCheck(
            name=f"REFERENTIAL_INTEGRITY({fk_column} -> {pk_column})",
            passed=orphans == 0,
            details={"orphan_fk_values": orphans},
        )

    @staticmethod
    def check_row_count(
        df: DataFrame,
        min_rows: int,
        table_name: str = "",
    ) -> QualityCheck:
        """Check that the DataFrame has at least *min_rows* rows."""
        n = df.count()
        return QualityCheck(
            name=f"ROW_COUNT({table_name or 'df'})",
            passed=n >= min_rows,
            details={"row_count": n, "min_expected": min_rows},
        )

    # ── batch runner ──────────────────────────────────────────────────────────

    @staticmethod
    def run_all(
        df: DataFrame,
        checks: list,
        table: str = "",
    ) -> QualityReport:
        """Run a list of check callables and return a QualityReport.

        Each callable must accept *df* and return a QualityCheck.

        Example:
            report = QualityChecks.run_all(df, [
                lambda d: QualityChecks.check_not_null(d, "transaction_id"),
                lambda d: QualityChecks.check_range(d, "amount_brl", 0, 1_000_000),
            ], table="fx_silver.transactions")
            report.raise_if_failed()
        """
        report = QualityReport(table=table)
        for check_fn in checks:
            result = check_fn(df)
            report.checks.append(result)
            logger.info(str(result))
        return report

    # Keep legacy name for any existing callers
    @staticmethod
    def execute_checks(df: DataFrame, checks: list) -> dict:
        report = QualityChecks.run_all(df, checks)
        return {c.name: c for c in report.checks}
