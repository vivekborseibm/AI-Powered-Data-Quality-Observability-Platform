"""
quality_checker.py — Reusable DataQualityChecker class.

Checks performed:
  - check_nulls
  - check_duplicates
  - check_schema
  - check_ranges
  - check_freshness
  - check_referential_integrity

CDC-aware behavior:
  - Optionally exclude soft-deleted rows from checks
  - Preserve compatibility with Bronze and Silver Delta tables carrying
    _is_deleted, _deleted_at, _row_hash, and lineage metadata

Results are collected as dicts and written to the quality_metrics Delta table.
"""

from __future__ import annotations

import datetime
from dataclasses import dataclass, field, asdict
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, NumericType


@dataclass
class MetricRecord:
    table_name: str
    layer: str
    check_name: str
    column_name: str
    metric_value: float
    threshold: float
    passed: bool
    run_timestamp: str = field(default_factory=lambda: datetime.datetime.utcnow().isoformat())
    details: str = ""


class DataQualityChecker:
    """Run a suite of data quality checks against a PySpark DataFrame."""

    def __init__(self, spark: SparkSession, metrics_table: str) -> None:
        self.spark = spark
        self.metrics_table = metrics_table

    def _prepare_df(self, df: DataFrame, include_deleted: bool = False) -> DataFrame:
        """Optionally exclude soft-deleted rows from quality checks."""
        if include_deleted:
            return df
        if "_is_deleted" in df.columns:
            return df.filter(~F.coalesce(F.col("_is_deleted"), F.lit(False)))
        return df

    # ── Public entry point ─────────────────────────────────────────────────────

    def run_all_checks(
        self,
        df: DataFrame,
        table_name: str,
        layer: str,
        expected_schema: StructType | None = None,
        key_columns: list[str] | None = None,
        timestamp_column: str | None = None,
        ref_checks: list[dict[str, Any]] | None = None,
        include_deleted: bool = False,
    ) -> list[MetricRecord]:
        prepared_df = self._prepare_df(df, include_deleted=include_deleted)

        results: list[MetricRecord] = []
        results += self.check_nulls(prepared_df, table_name, layer)
        results += self.check_duplicates(prepared_df, table_name, layer, key_columns=key_columns)
        if expected_schema:
            results += self.check_schema(prepared_df, table_name, layer, expected_schema)
        results += self.check_ranges(prepared_df, table_name, layer)
        if timestamp_column and timestamp_column in prepared_df.columns:
            results += self.check_freshness(prepared_df, table_name, layer, timestamp_column)
        if ref_checks:
            for rc in ref_checks:
                results += self.check_referential_integrity(prepared_df, table_name, layer, **rc)
        return results

    # ── Individual checks ──────────────────────────────────────────────────────

    def check_nulls(self, df: DataFrame, table_name: str, layer: str) -> list[MetricRecord]:
        from config import NULL_RATE_THRESHOLD

        records = []
        total = df.count()
        for col_name in df.columns:
            null_count = df.filter(F.col(col_name).isNull()).count()
            null_rate = null_count / total if total else 0.0
            records.append(MetricRecord(
                table_name=table_name,
                layer=layer,
                check_name="null_rate",
                column_name=col_name,
                metric_value=round(null_rate, 4),
                threshold=NULL_RATE_THRESHOLD,
                passed=null_rate <= NULL_RATE_THRESHOLD,
                details=f"{null_count} nulls out of {total} active rows",
            ))
        return records

    def check_duplicates(
        self,
        df: DataFrame,
        table_name: str,
        layer: str,
        key_columns: list[str] | None = None,
    ) -> list[MetricRecord]:
        from config import DUPLICATE_RATE_THRESHOLD

        cols = key_columns or [c for c in df.columns if not c.startswith("_")]
        total = df.count()
        distinct = df.dropDuplicates(cols).count()
        dup_rate = (total - distinct) / total if total else 0.0
        return [MetricRecord(
            table_name=table_name,
            layer=layer,
            check_name="duplicate_rate",
            column_name=",".join(cols),
            metric_value=round(dup_rate, 4),
            threshold=DUPLICATE_RATE_THRESHOLD,
            passed=dup_rate <= DUPLICATE_RATE_THRESHOLD,
            details=f"{total - distinct} duplicates out of {total} active rows",
        )]

    def check_schema(
        self,
        df: DataFrame,
        table_name: str,
        layer: str,
        expected_schema: StructType,
    ) -> list[MetricRecord]:
        records = []
        actual_fields = {f.name: str(f.dataType) for f in df.schema.fields}
        for expected_field in expected_schema.fields:
            present = expected_field.name in actual_fields
            type_match = present and actual_fields[expected_field.name] == str(expected_field.dataType)
            records.append(MetricRecord(
                table_name=table_name,
                layer=layer,
                check_name="schema_conformity",
                column_name=expected_field.name,
                metric_value=1.0 if type_match else 0.0,
                threshold=1.0,
                passed=type_match,
                details="" if type_match else f"expected {expected_field.dataType}, got {actual_fields.get(expected_field.name, 'MISSING')}",
            ))
        return records

    def check_ranges(self, df: DataFrame, table_name: str, layer: str) -> list[MetricRecord]:
        from config import OUTLIER_ZSCORE_THRESHOLD

        records = []
        numeric_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, NumericType)]
        total = df.count()
        for col_name in numeric_cols:
            stats = df.select(
                F.mean(col_name).alias("mean"),
                F.stddev(col_name).alias("std"),
            ).collect()[0]
            mean_val, std_val = stats["mean"], stats["std"]
            if std_val and std_val > 0:
                outlier_count = df.filter(
                    F.abs((F.col(col_name) - mean_val) / std_val) > OUTLIER_ZSCORE_THRESHOLD
                ).count()
                outlier_rate = outlier_count / total if total else 0.0
            else:
                outlier_rate = 0.0
            details = f"mean={mean_val}, std={std_val}" if std_val else "std=0"
            records.append(MetricRecord(
                table_name=table_name,
                layer=layer,
                check_name="outlier_rate",
                column_name=col_name,
                metric_value=round(outlier_rate, 4),
                threshold=0.01,
                passed=outlier_rate <= 0.01,
                details=details,
            ))
        return records

    def check_freshness(
        self,
        df: DataFrame,
        table_name: str,
        layer: str,
        timestamp_column: str,
    ) -> list[MetricRecord]:
        from config import FRESHNESS_MAX_HOURS

        latest = df.select(F.max(timestamp_column)).collect()[0][0]
        if latest is None:
            age_hours = float("inf")
        else:
            if isinstance(latest, str):
                latest = datetime.datetime.fromisoformat(latest)
            age_hours = (datetime.datetime.utcnow() - latest.replace(tzinfo=None)).total_seconds() / 3600
        return [MetricRecord(
            table_name=table_name,
            layer=layer,
            check_name="freshness_hours",
            column_name=timestamp_column,
            metric_value=round(age_hours, 2),
            threshold=FRESHNESS_MAX_HOURS,
            passed=age_hours <= FRESHNESS_MAX_HOURS,
            details=f"latest active record at {latest}",
        )]

    def check_referential_integrity(
        self,
        df: DataFrame,
        table_name: str,
        layer: str,
        fk_column: str,
        ref_table: str,
        ref_column: str,
        ref_include_deleted: bool = False,
    ) -> list[MetricRecord]:
        ref_df = self.spark.read.format("delta").table(ref_table)
        ref_df = self._prepare_df(ref_df, include_deleted=ref_include_deleted)
        total = df.count()
        candidate_df = df.filter(F.col(fk_column).isNotNull())
        orphans = candidate_df.join(ref_df, candidate_df[fk_column] == ref_df[ref_column], "left_anti").count()
        base_count = candidate_df.count()
        orphan_rate = orphans / base_count if base_count else 0.0
        return [MetricRecord(
            table_name=table_name,
            layer=layer,
            check_name="referential_integrity",
            column_name=fk_column,
            metric_value=round(orphan_rate, 4),
            threshold=0.0,
            passed=orphan_rate == 0.0,
            details=f"{orphans} orphan rows out of {base_count} checked against {ref_table}.{ref_column}",
        )]

    # ── Write results ──────────────────────────────────────────────────────────

    def write_metrics(self, metrics: list[MetricRecord]) -> None:
        rows = [asdict(m) for m in metrics]
        metrics_df = self.spark.createDataFrame(rows)
        metrics_df.write.format("delta").mode("append").saveAsTable(self.metrics_table)
