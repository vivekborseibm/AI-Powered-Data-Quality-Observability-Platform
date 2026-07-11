# Databricks notebook source
# MAGIC %md
# MAGIC # Quality Checks — Run DataQualityChecker across Bronze, Silver, and Gold
# MAGIC Executes quality checks across all curated Delta tables,
# MAGIC including CDC-aware filtering and AACT referential integrity checks,
# MAGIC writes results to quality_metrics, then generates an AI insight.

# COMMAND ----------

import sys
sys.path.insert(0, "../src")

from pyspark.sql import SparkSession
from quality_checker import DataQualityChecker
from ai_insights import generate_ai_insight
from config import (
    BRONZE_TABLES,
    SILVER_TABLES,
    GOLD_TABLE_STUDY_STATUS,
    GOLD_TABLE_PHASE_SUMMARY,
    GOLD_TABLE_CONDITION_INTERVENTION,
    QUALITY_METRICS_TABLE,
    AI_INSIGHTS_TABLE,
    TABLE_PRIMARY_KEYS,
)

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

checker = DataQualityChecker(spark, metrics_table=QUALITY_METRICS_TABLE)

STUDIES_SILVER = SILVER_TABLES["studies"]
OUTCOMES_SILVER = SILVER_TABLES["outcomes"]

TABLE_CONFIGS = {
    BRONZE_TABLES["studies"]: {
        "layer": "bronze_studies",
        "key_columns": TABLE_PRIMARY_KEYS["studies"],
        "timestamp_column": "last_update_posted_date",
        "include_deleted": False,
    },
    BRONZE_TABLES["sponsors"]: {
        "layer": "bronze_sponsors",
        "key_columns": TABLE_PRIMARY_KEYS["sponsors"],
        "timestamp_column": "_ingested_at",
        "include_deleted": False,
        "ref_checks": [{"fk_column": "nct_id", "ref_table": STUDIES_SILVER, "ref_column": "nct_id"}],
    },
    BRONZE_TABLES["conditions"]: {
        "layer": "bronze_conditions",
        "key_columns": TABLE_PRIMARY_KEYS["conditions"],
        "timestamp_column": "_ingested_at",
        "include_deleted": False,
        "ref_checks": [{"fk_column": "nct_id", "ref_table": STUDIES_SILVER, "ref_column": "nct_id"}],
    },
    BRONZE_TABLES["interventions"]: {
        "layer": "bronze_interventions",
        "key_columns": TABLE_PRIMARY_KEYS["interventions"],
        "timestamp_column": "_ingested_at",
        "include_deleted": False,
        "ref_checks": [{"fk_column": "nct_id", "ref_table": STUDIES_SILVER, "ref_column": "nct_id"}],
    },
    BRONZE_TABLES["eligibilities"]: {
        "layer": "bronze_eligibilities",
        "key_columns": TABLE_PRIMARY_KEYS["eligibilities"],
        "timestamp_column": "_ingested_at",
        "include_deleted": False,
        "ref_checks": [{"fk_column": "nct_id", "ref_table": STUDIES_SILVER, "ref_column": "nct_id"}],
    },
    BRONZE_TABLES["facilities"]: {
        "layer": "bronze_facilities",
        "key_columns": TABLE_PRIMARY_KEYS["facilities"],
        "timestamp_column": "_ingested_at",
        "include_deleted": False,
        "ref_checks": [{"fk_column": "nct_id", "ref_table": STUDIES_SILVER, "ref_column": "nct_id"}],
    },
    BRONZE_TABLES["outcomes"]: {
        "layer": "bronze_outcomes",
        "key_columns": TABLE_PRIMARY_KEYS["outcomes"],
        "timestamp_column": "_ingested_at",
        "include_deleted": False,
        "ref_checks": [{"fk_column": "nct_id", "ref_table": STUDIES_SILVER, "ref_column": "nct_id"}],
    },
    BRONZE_TABLES["outcome_analyses"]: {
        "layer": "bronze_outcome_analyses",
        "key_columns": TABLE_PRIMARY_KEYS["outcome_analyses"],
        "timestamp_column": "_ingested_at",
        "include_deleted": False,
        "ref_checks": [{"fk_column": "outcome_id", "ref_table": OUTCOMES_SILVER, "ref_column": "id"}],
    },
    SILVER_TABLES["studies"]: {
        "layer": "silver_studies",
        "key_columns": TABLE_PRIMARY_KEYS["studies"],
        "timestamp_column": "last_update_posted_date",
        "include_deleted": False,
    },
    SILVER_TABLES["sponsors"]: {
        "layer": "silver_sponsors",
        "key_columns": TABLE_PRIMARY_KEYS["sponsors"],
        "timestamp_column": "_ingested_at",
        "include_deleted": False,
        "ref_checks": [{"fk_column": "nct_id", "ref_table": STUDIES_SILVER, "ref_column": "nct_id"}],
    },
    SILVER_TABLES["conditions"]: {
        "layer": "silver_conditions",
        "key_columns": TABLE_PRIMARY_KEYS["conditions"],
        "timestamp_column": "_ingested_at",
        "include_deleted": False,
        "ref_checks": [{"fk_column": "nct_id", "ref_table": STUDIES_SILVER, "ref_column": "nct_id"}],
    },
    SILVER_TABLES["interventions"]: {
        "layer": "silver_interventions",
        "key_columns": TABLE_PRIMARY_KEYS["interventions"],
        "timestamp_column": "_ingested_at",
        "include_deleted": False,
        "ref_checks": [{"fk_column": "nct_id", "ref_table": STUDIES_SILVER, "ref_column": "nct_id"}],
    },
    SILVER_TABLES["eligibilities"]: {
        "layer": "silver_eligibilities",
        "key_columns": TABLE_PRIMARY_KEYS["eligibilities"],
        "timestamp_column": "_ingested_at",
        "include_deleted": False,
        "ref_checks": [{"fk_column": "nct_id", "ref_table": STUDIES_SILVER, "ref_column": "nct_id"}],
    },
    SILVER_TABLES["facilities"]: {
        "layer": "silver_facilities",
        "key_columns": TABLE_PRIMARY_KEYS["facilities"],
        "timestamp_column": "_ingested_at",
        "include_deleted": False,
        "ref_checks": [{"fk_column": "nct_id", "ref_table": STUDIES_SILVER, "ref_column": "nct_id"}],
    },
    SILVER_TABLES["outcomes"]: {
        "layer": "silver_outcomes",
        "key_columns": TABLE_PRIMARY_KEYS["outcomes"],
        "timestamp_column": "_ingested_at",
        "include_deleted": False,
        "ref_checks": [{"fk_column": "nct_id", "ref_table": STUDIES_SILVER, "ref_column": "nct_id"}],
    },
    SILVER_TABLES["outcome_analyses"]: {
        "layer": "silver_outcome_analyses",
        "key_columns": TABLE_PRIMARY_KEYS["outcome_analyses"],
        "timestamp_column": "_ingested_at",
        "include_deleted": False,
        "ref_checks": [{"fk_column": "outcome_id", "ref_table": OUTCOMES_SILVER, "ref_column": "id"}],
    },
    GOLD_TABLE_STUDY_STATUS: {
        "layer": "gold_study_status",
        "key_columns": ["overall_status"],
        "timestamp_column": None,
        "include_deleted": False,
    },
    GOLD_TABLE_PHASE_SUMMARY: {
        "layer": "gold_phase_summary",
        "key_columns": ["phase", "study_type"],
        "timestamp_column": None,
        "include_deleted": False,
    },
    GOLD_TABLE_CONDITION_INTERVENTION: {
        "layer": "gold_condition_intervention",
        "key_columns": ["condition_name", "intervention_type"],
        "timestamp_column": None,
        "include_deleted": False,
    },
}

all_metrics = []
for table_name, cfg in TABLE_CONFIGS.items():
    df = spark.read.format("delta").table(table_name)
    metrics = checker.run_all_checks(
        df,
        table_name=table_name,
        layer=cfg["layer"],
        key_columns=cfg.get("key_columns"),
        timestamp_column=cfg.get("timestamp_column"),
        ref_checks=cfg.get("ref_checks"),
        include_deleted=cfg.get("include_deleted", False),
    )
    all_metrics.extend(metrics)

checker.write_metrics(all_metrics)
print(f"Quality metrics written to → {QUALITY_METRICS_TABLE}")

# COMMAND ----------

metrics_df = spark.read.format("delta").table(QUALITY_METRICS_TABLE)
generate_ai_insight(spark, metrics_df, insights_table=AI_INSIGHTS_TABLE)
print(f"AI insights written to → {AI_INSIGHTS_TABLE}")
