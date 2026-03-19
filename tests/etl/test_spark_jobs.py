# Unit tests for ETL jobs and common utilities
# Uses the local SparkSession from conftest.py (no cluster required).
# Run: pytest tests/etl/ -m unit -v

from datetime import date, datetime
from decimal import Decimal

import pytest
from pyspark.sql import Row

# ── quality checks ────────────────────────────────────────────────────────────

@pytest.mark.unit
class TestQualityChecks:

    def test_check_not_null_passes(self, spark):
        from etl.common.quality import QualityChecks
        df = spark.createDataFrame([Row(id="a"), Row(id="b")])
        result = QualityChecks.check_not_null(df, "id")
        assert result.passed
        assert result.details["null_count"] == 0

    def test_check_not_null_fails(self, spark):
        from etl.common.quality import QualityChecks
        df = spark.createDataFrame([Row(id="a"), Row(id=None)])
        result = QualityChecks.check_not_null(df, "id")
        assert not result.passed
        assert result.details["null_count"] == 1

    def test_check_unique_passes(self, spark):
        from etl.common.quality import QualityChecks
        df = spark.createDataFrame([Row(id="a"), Row(id="b")])
        assert QualityChecks.check_unique(df, "id").passed

    def test_check_unique_fails(self, spark):
        from etl.common.quality import QualityChecks
        df = spark.createDataFrame([Row(id="a"), Row(id="a")])
        result = QualityChecks.check_unique(df, "id")
        assert not result.passed
        assert result.details["duplicates"] == 1

    def test_check_range_passes(self, spark):
        from etl.common.quality import QualityChecks
        df = spark.createDataFrame([Row(v=5.0), Row(v=10.0)])
        assert QualityChecks.check_range(df, "v", 0.0, 100.0).passed

    def test_check_range_fails(self, spark):
        from etl.common.quality import QualityChecks
        df = spark.createDataFrame([Row(v=5.0), Row(v=-1.0)])
        result = QualityChecks.check_range(df, "v", 0.0, 100.0)
        assert not result.passed
        assert result.details["out_of_range_count"] == 1

    def test_check_allowed_values_passes(self, spark):
        from etl.common.quality import QualityChecks
        df = spark.createDataFrame([Row(c="USD"), Row(c="EUR")])
        assert QualityChecks.check_allowed_values(df, "c", {"USD", "EUR", "GBP"}).passed

    def test_check_allowed_values_fails(self, spark):
        from etl.common.quality import QualityChecks
        df = spark.createDataFrame([Row(c="USD"), Row(c="BAD")])
        result = QualityChecks.check_allowed_values(df, "c", {"USD", "EUR"})
        assert not result.passed
        assert result.details["invalid_count"] == 1

    def test_check_referential_integrity_passes(self, spark):
        from etl.common.quality import QualityChecks
        txns = spark.createDataFrame([Row(user_id="u1"), Row(user_id="u2")])
        users = spark.createDataFrame([Row(user_id="u1"), Row(user_id="u2")])
        assert QualityChecks.check_referential_integrity(txns, "user_id", users, "user_id").passed

    def test_check_referential_integrity_fails(self, spark):
        from etl.common.quality import QualityChecks
        txns = spark.createDataFrame([Row(user_id="u1"), Row(user_id="ghost")])
        users = spark.createDataFrame([Row(user_id="u1")])
        result = QualityChecks.check_referential_integrity(txns, "user_id", users, "user_id")
        assert not result.passed
        assert result.details["orphan_fk_values"] == 1

    def test_quality_report_raise_if_failed(self, spark):
        from etl.common.quality import QualityChecks
        df = spark.createDataFrame([(None,)], schema="id STRING")
        report = QualityChecks.run_all(
            df, [lambda d: QualityChecks.check_not_null(d, "id")], table="t"
        )
        assert not report.passed
        with pytest.raises(ValueError, match="Quality checks FAILED"):
            report.raise_if_failed()

    def test_quality_report_summary_structure(self, spark):
        from etl.common.quality import QualityChecks
        df = spark.createDataFrame([Row(id="a")])
        report = QualityChecks.run_all(
            df, [lambda d: QualityChecks.check_not_null(d, "id")], table="t"
        )
        s = report.summary()
        assert s["overall"] == "PASS"
        assert s["total_checks"] == 1
        assert s["failed"] == 0


# ── schemas ────────────────────────────────────────────────────────────────────

@pytest.mark.unit
class TestSchemas:

    def test_transaction_schema_required_fields(self):
        from etl.common.schemas import Schemas
        names = {f.name for f in Schemas.TRANSACTION.fields}
        for col in ("transaction_id", "user_id", "timestamp", "currency",
                    "amount_brl", "exchange_rate"):
            assert col in names, f"Missing column: {col}"

    def test_create_df_with_transaction_schema(self, spark):
        from etl.common.schemas import Schemas
        data = [(
            "tr-1", "user-1", datetime(2024, 3, 15, 10, 0, 0),
            "BUY", "USD",
            Decimal("1000.00"), Decimal("200.0000"),
            Decimal("5.0000"), Decimal("0.010"),
            Decimal("10.00"), "completed", "web", "device1",
        )]
        df = spark.createDataFrame(data, schema=Schemas.TRANSACTION)
        assert df.count() == 1


# ── bronze deduplication ───────────────────────────────────────────────────────

@pytest.mark.unit
class TestBronzeIngestionJob:

    _SCHEMA = (
        "transaction_id STRING, user_id STRING, timestamp TIMESTAMP, "
        "transaction_type STRING, currency STRING, "
        "amount_brl DECIMAL(15,2), amount_foreign DECIMAL(15,4), "
        "exchange_rate DECIMAL(10,4), spread_pct DECIMAL(5,3), "
        "fee_brl DECIMAL(10,2), status STRING, channel STRING, device STRING, "
        "is_anomaly BOOLEAN, anomaly_score DECIMAL(5,4)"
    )

    def _df(self, spark):
        return spark.createDataFrame(
            [
                ("tx-1", "u1", datetime(2024, 3, 15, 10, 0, 0), "BUY", "USD",
                 Decimal("1000.00"), Decimal("200.0000"), Decimal("5.0000"),
                 Decimal("0.010"), Decimal("10.00"), "completed", "web", "d1",
                 False, Decimal("0.0000")),
                # duplicate id, newer timestamp — should be kept
                ("tx-1", "u1", datetime(2024, 3, 15, 11, 0, 0), "BUY", "USD",
                 Decimal("1000.00"), Decimal("200.0000"), Decimal("5.0000"),
                 Decimal("0.010"), Decimal("10.00"), "completed", "web", "d1",
                 False, Decimal("0.0000")),
                ("tx-2", "u2", datetime(2024, 3, 15, 12, 0, 0), "SELL", "EUR",
                 Decimal("500.00"), Decimal("100.0000"), Decimal("5.0000"),
                 Decimal("0.010"), Decimal("5.00"), "completed", "mobile", "d2",
                 False, Decimal("0.0000")),
            ],
            schema=self._SCHEMA,
        )

    def test_deduplicate_keeps_latest(self, spark):
        from etl.jobs.bronze.ingest_transactions import BronzeIngestionJob
        df = self._df(spark)
        job = BronzeIngestionJob(spark)
        result = job.deduplicate(df)
        assert result.count() == 2

    def test_deduplicate_keeps_newer_timestamp(self, spark):
        from etl.jobs.bronze.ingest_transactions import BronzeIngestionJob
        df = self._df(spark)
        job = BronzeIngestionJob(spark)
        result = job.deduplicate(df)
        tx1 = [r for r in result.collect() if r.transaction_id == "tx-1"][0]
        assert str(tx1.timestamp) == "2024-03-15 11:00:00"

    def test_add_audit_columns(self, spark):
        from etl.jobs.bronze.ingest_transactions import BronzeIngestionJob
        df = self._df(spark)
        job = BronzeIngestionJob(spark)
        result = job.add_audit_columns(df)
        assert "_bronze_loaded_at" in result.columns
        assert "_source_file" in result.columns
        assert "_batch_id" in result.columns


# ── silver clean + enrich ──────────────────────────────────────────────────────

@pytest.mark.unit
class TestSilverTransformationJob:

    _SCHEMA = (
        "transaction_id STRING, user_id STRING, timestamp TIMESTAMP, "
        "transaction_type STRING, currency STRING, "
        "amount_brl DECIMAL(15,2), amount_foreign DECIMAL(15,4), "
        "exchange_rate DECIMAL(10,4), spread_pct DECIMAL(5,3), "
        "fee_brl DECIMAL(10,2), status STRING, channel STRING, device STRING"
    )

    def _df(self, spark):
        return spark.createDataFrame(
            [
                ("tx-1", "u1", datetime(2024, 3, 15, 10, 0, 0), "BUY", "USD",
                 Decimal("1000.00"), Decimal("200.0000"), Decimal("5.0000"),
                 Decimal("0.010"), Decimal("10.00"), "completed", "web", "d1"),
                # invalid currency
                ("tx-2", "u2", datetime(2024, 3, 15, 11, 0, 0), "SELL", "INVALID",
                 Decimal("500.00"), Decimal("100.0000"), Decimal("5.0000"),
                 Decimal("0.010"), None, "completed", "mobile", "d2"),
                # negative amount
                ("tx-3", "u3", datetime(2024, 3, 15, 12, 0, 0), "BUY", "EUR",
                 Decimal("-100.00"), Decimal("20.0000"), Decimal("5.0000"),
                 Decimal("0.010"), None, "completed", "api", "d3"),
            ],
            schema=self._SCHEMA,
        )

    def test_clean_removes_invalid_currency(self, spark):
        from etl.jobs.silver.transform_transactions import SilverTransformationJob
        df = self._df(spark)
        result = SilverTransformationJob(spark).clean(df)
        currencies = {r.currency for r in result.collect()}
        assert "INVALID" not in currencies

    def test_clean_removes_negative_amounts(self, spark):
        from etl.jobs.silver.transform_transactions import SilverTransformationJob
        df = self._df(spark)
        result = SilverTransformationJob(spark).clean(df)
        for row in result.collect():
            assert row.amount_brl > 0

    def test_enrich_adds_temporal_columns(self, spark):
        from etl.jobs.silver.transform_transactions import SilverTransformationJob
        df = self._df(spark).filter("transaction_id = 'tx-1'")
        result = SilverTransformationJob(spark).enrich(df)
        for col in ("amount_usd", "day_of_week", "hour_of_day", "is_business_hours", "date"):
            assert col in result.columns, f"Missing column: {col}"

    def test_validate_raises_when_all_negative(self, spark):
        from etl.jobs.silver.transform_transactions import SilverTransformationJob
        df = spark.createDataFrame(
            [("tx-X", "u1", datetime(2024, 1, 1, 10, 0, 0), "BUY", "USD", Decimal("-1.00"))],
            schema="transaction_id STRING, user_id STRING, timestamp TIMESTAMP, "
                   "transaction_type STRING, currency STRING, amount_brl DECIMAL(15,2)",
        )
        with pytest.raises(ValueError, match="Quality check FAILED"):
            SilverTransformationJob(spark).validate(df)


# ── gold aggregations ──────────────────────────────────────────────────────────

@pytest.mark.unit
class TestGoldAggregationJob:

    def _silver_df(self, spark):
        return spark.createDataFrame(
            [
                ("tx-1", "u1", datetime(2024, 3, 15, 10, 0, 0), "USD",
                 Decimal("1000.00"), Decimal("5.0000"), date(2024, 3, 15)),
                ("tx-2", "u1", datetime(2024, 3, 15, 14, 0, 0), "USD",
                 Decimal("2000.00"), Decimal("5.0000"), date(2024, 3, 15)),
                ("tx-3", "u2", datetime(2024, 3, 15, 20, 0, 0), "EUR",
                 Decimal("500.00"), Decimal("5.5000"), date(2024, 3, 15)),
            ],
            schema="transaction_id STRING, user_id STRING, timestamp TIMESTAMP, "
                   "currency STRING, amount_brl DECIMAL(15,2), "
                   "exchange_rate DECIMAL(10,4), date DATE",
        )

    def test_daily_volume_row_count(self, spark):
        from unittest.mock import patch

        from etl.jobs.gold.aggregate_metrics import GoldAggregationJob

        job = GoldAggregationJob(spark)
        with patch.object(job, "_silver_df", return_value=self._silver_df(spark)):
            result = job.create_daily_volume()

        assert result.count() == 2  # USD and EUR

    def test_daily_volume_usd_count(self, spark):
        from unittest.mock import patch

        from etl.jobs.gold.aggregate_metrics import GoldAggregationJob

        job = GoldAggregationJob(spark)
        with patch.object(job, "_silver_df", return_value=self._silver_df(spark)):
            result = job.create_daily_volume()

        usd_row = result.filter("currency = 'USD'").collect()[0]
        assert int(usd_row.transaction_count) == 2

    def test_user_summary_groups_correctly(self, spark):
        from unittest.mock import patch

        from etl.jobs.gold.aggregate_metrics import GoldAggregationJob

        job = GoldAggregationJob(spark)
        with patch.object(job, "_silver_df", return_value=self._silver_df(spark)):
            result = job.create_user_summary()

        assert result.count() == 2  # u1 and u2
