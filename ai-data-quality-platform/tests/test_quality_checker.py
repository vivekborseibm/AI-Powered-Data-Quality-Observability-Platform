"""
test_quality_checker.py — Unit tests for DataQualityChecker.

Run locally using pytest + pyspark local mode:
    pip install pytest pyspark
    pytest tests/test_quality_checker.py -v
"""

import datetime
import os
import sys
import unittest.mock as mock

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Make src importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../src"))

# Patch config imports used inside quality_checker before importing it
config_patch = {
    "NULL_RATE_THRESHOLD": 0.05,
    "DUPLICATE_RATE_THRESHOLD": 0.02,
    "FRESHNESS_MAX_HOURS": 24,
    "OUTLIER_ZSCORE_THRESHOLD": 3.0,
}
with mock.patch.dict("sys.modules", {"config": mock.MagicMock(**config_patch)}):
    from quality_checker import DataQualityChecker


@pytest.fixture(scope="session")
def spark():
    spark_session = (
        SparkSession.builder
        .master("local[1]")
        .appName("test_quality_checker")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield spark_session
    spark_session.stop()


@pytest.fixture()
def cdc_df(spark):
    now = datetime.datetime.utcnow()
    schema = StructType([
        StructField("nct_id", StringType(), True),
        StructField("brief_title", StringType(), True),
        StructField("enrollment", IntegerType(), True),
        StructField("last_update_posted_date", TimestampType(), True),
        StructField("_is_deleted", BooleanType(), True),
        StructField("_ingested_at", TimestampType(), True),
        StructField("_row_hash", StringType(), True),
    ])
    data = [
        ("NCT001", "Study A", 100, now, False, now, "hash1"),
        ("NCT002", None, 200, now, False, now, "hash2"),
        ("NCT002", None, 200, now, False, now, "hash2"),
        ("NCT003", "Study C", 300, now, True, now, "hash3"),
    ]
    return spark.createDataFrame(data, schema)


@pytest.fixture()
def studies_df(spark):
    now = datetime.datetime.utcnow()
    schema = StructType([
        StructField("nct_id", StringType(), True),
        StructField("last_update_posted_date", TimestampType(), True),
        StructField("_is_deleted", BooleanType(), True),
    ])
    data = [
        ("NCT001", now, False),
        ("NCT002", now, False),
        ("NCT003", now, True),
    ]
    return spark.createDataFrame(data, schema)


@pytest.fixture()
def sponsors_df(spark):
    schema = StructType([
        StructField("nct_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("lead_or_collaborator", StringType(), True),
        StructField("_is_deleted", BooleanType(), True),
    ])
    data = [
        ("NCT001", "Sponsor A", "lead", False),
        ("NCT999", "Sponsor B", "collaborator", False),
        ("NCT003", "Sponsor C", "lead", True),
    ]
    return spark.createDataFrame(data, schema)


class TestPrepareDf:
    def test_excludes_soft_deleted_rows_by_default(self, spark, cdc_df):
        checker = DataQualityChecker(spark, metrics_table="dummy")
        prepared = checker._prepare_df(cdc_df)
        assert prepared.count() == 3
        assert prepared.filter(prepared._is_deleted == True).count() == 0  # noqa: E712

    def test_can_include_deleted_rows(self, spark, cdc_df):
        checker = DataQualityChecker(spark, metrics_table="dummy")
        prepared = checker._prepare_df(cdc_df, include_deleted=True)
        assert prepared.count() == 4


class TestCheckNulls:
    def test_detects_nulls_on_active_rows_only(self, spark, cdc_df):
        checker = DataQualityChecker(spark, metrics_table="dummy")
        prepared = checker._prepare_df(cdc_df)
        results = checker.check_nulls(prepared, "test_table", "silver")
        brief_title_result = next(r for r in results if r.column_name == "brief_title")
        assert brief_title_result.metric_value == pytest.approx(2 / 3, rel=1e-3)
        assert brief_title_result.passed is False


class TestCheckDuplicates:
    def test_detects_duplicate_active_rows_on_business_key(self, spark, cdc_df):
        checker = DataQualityChecker(spark, metrics_table="dummy")
        prepared = checker._prepare_df(cdc_df)
        results = checker.check_duplicates(prepared, "test_table", "silver", key_columns=["nct_id"])
        assert results[0].passed is False
        assert results[0].metric_value == pytest.approx(1 / 3, rel=1e-3)

    def test_ignores_metadata_columns_when_no_key_columns_supplied(self, spark):
        checker = DataQualityChecker(spark, metrics_table="dummy")
        now = datetime.datetime.utcnow()
        df = spark.createDataFrame(
            [
                ("A", 1, False, now),
                ("A", 1, False, now),
            ],
            ["id", "value", "_is_deleted", "_ingested_at"],
        )
        results = checker.check_duplicates(df, "test_table", "bronze")
        assert results[0].column_name == "id,value"
        assert results[0].metric_value == pytest.approx(0.5, rel=1e-3)


class TestCheckSchema:
    def test_flags_missing_column(self, spark, cdc_df):
        checker = DataQualityChecker(spark, metrics_table="dummy")
        expected = StructType([
            StructField("nct_id", StringType(), True),
            StructField("missing_column", StringType(), True),
        ])
        results = checker.check_schema(cdc_df, "test_table", "silver", expected)
        missing = next(r for r in results if r.column_name == "missing_column")
        assert missing.passed is False

    def test_passes_matching_schema(self, spark, cdc_df):
        checker = DataQualityChecker(spark, metrics_table="dummy")
        expected = StructType([StructField("nct_id", StringType(), True)])
        results = checker.check_schema(cdc_df, "test_table", "silver", expected)
        assert results[0].passed is True


class TestCheckFreshness:
    def test_uses_timestamp_column(self, spark, studies_df):
        checker = DataQualityChecker(spark, metrics_table="dummy")
        prepared = checker._prepare_df(studies_df)
        results = checker.check_freshness(prepared, "studies", "silver", "last_update_posted_date")
        assert results[0].column_name == "last_update_posted_date"
        assert results[0].metric_value >= 0


class TestReferentialIntegrity:
    def test_detects_orphans_against_active_reference_rows(self, spark, studies_df, sponsors_df):
        checker = DataQualityChecker(spark, metrics_table="dummy")

        studies_df.write.format("delta").mode("overwrite").saveAsTable("default.test_studies_ref")

        prepared_sponsors = checker._prepare_df(sponsors_df)
        results = checker.check_referential_integrity(
            prepared_sponsors,
            table_name="default.test_sponsors",
            layer="silver_sponsors",
            fk_column="nct_id",
            ref_table="default.test_studies_ref",
            ref_column="nct_id",
        )
        assert results[0].passed is False
        assert results[0].metric_value == pytest.approx(0.5, rel=1e-3)
        assert "orphan rows" in results[0].details


class TestRunAllChecks:
    def test_runs_with_cdc_filtering_and_timestamp(self, spark, cdc_df):
        checker = DataQualityChecker(spark, metrics_table="dummy")
        results = checker.run_all_checks(
            cdc_df,
            table_name="dq_project.silver_studies",
            layer="silver_studies",
            key_columns=["nct_id"],
            timestamp_column="last_update_posted_date",
            include_deleted=False,
        )
        check_names = {r.check_name for r in results}
        assert "null_rate" in check_names
        assert "duplicate_rate" in check_names
        assert "freshness_hours" in check_names

    def test_can_include_deleted_in_run_all_checks(self, spark, cdc_df):
        checker = DataQualityChecker(spark, metrics_table="dummy")
        results = checker.run_all_checks(
            cdc_df,
            table_name="dq_project.bronze_studies",
            layer="bronze_studies",
            key_columns=["nct_id"],
            include_deleted=True,
        )
        duplicate_metric = next(r for r in results if r.check_name == "duplicate_rate")
        assert duplicate_metric.metric_value >= 0
