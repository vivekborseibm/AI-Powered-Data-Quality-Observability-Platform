# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer — Clinical Trial Aggregates
# MAGIC Reads Silver AACT tables and produces business-facing aggregates for dashboarding.

# COMMAND ----------

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, countDistinct

sys.path.insert(0, "../src")
from config import (
    SILVER_TABLES,
    GOLD_TABLE_STUDY_STATUS,
    GOLD_TABLE_PHASE_SUMMARY,
    GOLD_TABLE_CONDITION_INTERVENTION,
)

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

studies_sl = spark.read.format("delta").table(SILVER_TABLES["studies"])
conditions_sl = spark.read.format("delta").table(SILVER_TABLES["conditions"])
interventions_sl = spark.read.format("delta").table(SILVER_TABLES["interventions"])

# ── Study status summary ───────────────────────────────────────────────────────
study_status_df = (
    studies_sl
    .groupBy("overall_status")
    .agg(countDistinct("nct_id").alias("study_count"))
    .orderBy(col("study_count").desc())
)
study_status_df.write.format("delta").mode("overwrite").saveAsTable(GOLD_TABLE_STUDY_STATUS)
print(f"Gold aggregate complete → {GOLD_TABLE_STUDY_STATUS}")

# ── Phase summary ──────────────────────────────────────────────────────────────
phase_summary_df = (
    studies_sl
    .groupBy("phase", "study_type")
    .agg(countDistinct("nct_id").alias("study_count"))
    .orderBy(col("study_count").desc())
)
phase_summary_df.write.format("delta").mode("overwrite").saveAsTable(GOLD_TABLE_PHASE_SUMMARY)
print(f"Gold aggregate complete → {GOLD_TABLE_PHASE_SUMMARY}")

# ── Condition / intervention coverage summary ─────────────────────────────────
condition_intervention_df = (
    studies_sl.select("nct_id")
    .join(conditions_sl.select("nct_id", "name"), on="nct_id", how="left")
    .join(interventions_sl.select("nct_id", "intervention_type"), on="nct_id", how="left")
    .groupBy("name", "intervention_type")
    .agg(countDistinct("nct_id").alias("study_count"))
    .withColumnRenamed("name", "condition_name")
    .orderBy(col("study_count").desc())
)
condition_intervention_df.write.format("delta").mode("overwrite").saveAsTable(GOLD_TABLE_CONDITION_INTERVENTION)
print(f"Gold aggregate complete → {GOLD_TABLE_CONDITION_INTERVENTION}")
