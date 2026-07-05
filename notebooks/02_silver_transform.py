# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer — Deduplication, Type Casting, Null Handling
# MAGIC Reads from Bronze, applies cleaning rules, writes to Silver Delta table.

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, trim
import sys
sys.path.insert(0, "../src")
from config import BRONZE_TABLE, SILVER_TABLE

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

bronze_df = spark.read.format("delta").table(BRONZE_TABLE)

# Deduplicate on order_id
deduped_df = bronze_df.dropDuplicates(["order_id"])

# Cast types and standardize
silver_df = (
    deduped_df
    .withColumn("order_date", to_timestamp(col("order_date"), "yyyy-MM-dd"))
    .withColumn("amount",     col("amount").cast("double"))
    .withColumn("customer_id", col("customer_id").cast("integer"))
    .withColumn("product_id",  col("product_id").cast("integer"))
    .withColumn("status",      trim(col("status")))
    # Drop rows where primary key is null
    .filter(col("order_id").isNotNull())
)

# Write to Silver Delta table
silver_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(SILVER_TABLE)

print(f"Silver transform complete → {SILVER_TABLE}")
