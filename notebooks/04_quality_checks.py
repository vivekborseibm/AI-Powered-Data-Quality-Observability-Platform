# Databricks notebook source
# MAGIC %md
# MAGIC # Quality Checks — Run DataQualityChecker across all layers
# MAGIC Executes all quality checks, writes results to quality_metrics Delta table,
# MAGIC then calls the AI insights layer to generate a plain-English summary.

# COMMAND ----------

import sys
sys.path.insert(0, "../src")
from quality_checker import DataQualityChecker
from ai_insights import generate_ai_insight
from config import (
    BRONZE_TABLE, SILVER_TABLE,
    GOLD_TABLE_DAILY_REVENUE, GOLD_TABLE_CUSTOMER_LTV,
    QUALITY_METRICS_TABLE, AI_INSIGHTS_TABLE,
)
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

checker = DataQualityChecker(spark, metrics_table=QUALITY_METRICS_TABLE)

tables_to_check = {
    "bronze": BRONZE_TABLE,
    "silver": SILVER_TABLE,
    "gold_daily_revenue": GOLD_TABLE_DAILY_REVENUE,
    "gold_customer_ltv":  GOLD_TABLE_CUSTOMER_LTV,
}

all_metrics = []
for layer, table in tables_to_check.items():
    df = spark.read.format("delta").table(table)
    metrics = checker.run_all_checks(df, table_name=table, layer=layer)
    all_metrics.extend(metrics)

checker.write_metrics(all_metrics)
print(f"Quality metrics written to → {QUALITY_METRICS_TABLE}")

# COMMAND ----------

# AI Insights — feed latest metrics to LLM
metrics_df = spark.read.format("delta").table(QUALITY_METRICS_TABLE)
generate_ai_insight(spark, metrics_df, insights_table=AI_INSIGHTS_TABLE)
print(f"AI insights written to → {AI_INSIGHTS_TABLE}")
