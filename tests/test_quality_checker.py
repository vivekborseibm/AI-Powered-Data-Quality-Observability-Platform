"""
test_quality_checker.py — Unit tests for DataQualityChecker.

Run locally (no Spark cluster needed) using pytest + pyspark local mode:
    pip install pytest pyspark
    pytest tests/test_quality_checker.py -v
"""

import datetime
import sys
import os

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, TimestampType,
)

# Make src importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../src"))

# Patch config imports used inside quality_checker before importing it
import unittest.mock as mock
config_patch = {
    "NULL_RATE_THRESHOLD":       0.05,
    "DUPLICATE_RATE_THRESHOLD":  0.02,
    "FRESHNESS_MAX_HOURS":       24,
    "OUTLIER_ZSCORE_THRESHOLD":  3.0,
}
with mock.patch.dict("sys.modules", {"config": mock.MagicMock(**config_patch)}):
    from quality_checker import DataQualityChecker


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local[1]")
        .appName("test_quality_checker")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )


@pytest.fixture()
def sample_df(spark):
    schema = StructType([
        StructField("order_id",   IntegerType(),   True),
        StructField("customer_id", IntegerType(),  True),
        StructField("amount",      DoubleType(),   True),
        StructField("status",      StringType(),   True),
        StructField("order_date",  TimestampType(), True),
    ])
    now = datetime.datetime.utcnow()
    data = [
        (1, 100, 50.0,  "completed", now),
        (2, 101, 75.0,  "pending",   now),
        (3, None, 30.0, "cancelled", now),   # null customer_id
        (4, 102, None,  "completed", now),   # null amount
        (2, 101, 75.0,  "pending",   now),   # duplicate of row 2
    ]
    return spark.createDataFrame(data, schema)


class TestCheckNulls:
    def test_detects_null_in_customer_id(self, spark, sample_df):
        checker = DataQualityChecker(spark, metrics_table="dummy")
        with mock.patch("quality_checker.config") as cfg:
            cfg.NULL_RATE_THRESHOLD = 0.05
            results = checker.check_nulls(sample_df, "test_table", "silver")
        customer_result = next(r for r in results if r.column_name == "customer_id")
        assert customer_result.metric_value == pytest.approx(0.2)
        assert customer_result.passed is False

    def test_passes_when_no_nulls(self, spark, sample_df):
        checker = DataQualityChecker(spark, metrics_table="dummy")
        clean_df = sample_df.dropna()
        with mock.patch("quality_checker.config") as cfg:
            cfg.NULL_RATE_THRESHOLD = 0.05
            results = checker.check_nulls(clean_df, "test_table", "silver")
        assert all(r.passed for r in results)


class TestCheckDuplicates:
    def test_detects_duplicate_rows(self, spark, sample_df):
        checker = DataQualityChecker(spark, metrics_table="dummy")
        with mock.patch("quality_checker.config") as cfg:
            cfg.DUPLICATE_RATE_THRESHOLD = 0.02
            results = checker.check_duplicates(sample_df, "test_table", "silver", key_columns=["order_id"])
        assert results[0].passed is False
        assert results[0].metric_value > 0

    def test_no_duplicates_on_clean_data(self, spark, spark_session=None):
        _spark = spark
        checker = DataQualityChecker(_spark, metrics_table="dummy")
        data = [(1, "a"), (2, "b"), (3, "c")]
        df = _spark.createDataFrame(data, ["id", "val"])
        with mock.patch("quality_checker.config") as cfg:
            cfg.DUPLICATE_RATE_THRESHOLD = 0.02
            results = checker.check_duplicates(df, "test_table", "silver", key_columns=["id"])
        assert results[0].passed is True
        assert results[0].metric_value == 0.0


class TestCheckSchema:
    def test_flags_missing_column(self, spark, sample_df):
        from pyspark.sql.types import StructType, StructField, StringType
        checker = DataQualityChecker(spark, metrics_table="dummy")
        expected = StructType([
            StructField("order_id",      IntegerType(), True),
            StructField("missing_column", StringType(), True),  # does not exist
        ])
        results = checker.check_schema(sample_df, "test_table", "silver", expected)
        missing = next(r for r in results if r.column_name == "missing_column")
        assert missing.passed is False

    def test_passes_matching_schema(self, spark, sample_df):
        checker = DataQualityChecker(spark, metrics_table="dummy")
        expected = StructType([StructField("order_id", IntegerType(), True)])
        results = checker.check_schema(sample_df, "test_table", "silver", expected)
        assert results[0].passed is True
