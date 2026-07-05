# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer — Business Aggregates
# MAGIC Reads from Silver, produces business-level aggregates, writes to Gold Delta table.

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, count, countDistinct, to_date
import sys
sys.path.insert(0, "../src")
from config import SILVER_TABLE, GOLD_TABLE_DAILY_REVENUE, GOLD_TABLE_CUSTOMER_LTV

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

silver_df = spark.read.format("delta").table(SILVER_TABLE)

# ── Daily Revenue ──────────────────────────────────────────────────────────────
daily_revenue_df = (
    silver_df
    .withColumn("order_date_day", to_date(col("order_date")))
    .groupBy("order_date_day")
    .agg(
        _sum("amount").alias("total_revenue"),
        count("order_id").alias("order_count"),
    )
    .orderBy("order_date_day")
)

daily_revenue_df.write.format("delta").mode("overwrite").saveAsTable(GOLD_TABLE_DAILY_REVENUE)
print(f"Gold daily revenue complete → {GOLD_TABLE_DAILY_REVENUE}")

# ── Customer LTV ───────────────────────────────────────────────────────────────
customer_ltv_df = (
    silver_df
    .groupBy("customer_id")
    .agg(
        _sum("amount").alias("lifetime_value"),
        count("order_id").alias("total_orders"),
        countDistinct("product_id").alias("unique_products"),
    )
)

customer_ltv_df.write.format("delta").mode("overwrite").saveAsTable(GOLD_TABLE_CUSTOMER_LTV)
print(f"Gold customer LTV complete → {GOLD_TABLE_CUSTOMER_LTV}")
