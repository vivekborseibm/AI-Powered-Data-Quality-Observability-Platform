# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer — Raw Ingestion
# MAGIC Reads raw synthetic CSV files, attaches ingestion metadata,
# MAGIC and writes to the Bronze Delta table.

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name
import sys
sys.path.insert(0, "../src")
from config import BRONZE_TABLE, RAW_DATA_PATH

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# Read raw source files
raw_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(RAW_DATA_PATH)
    .withColumn("_source_file", input_file_name())
    .withColumn("_ingested_at", current_timestamp())
)

# Write to Bronze Delta table
raw_df.write.format("delta").mode("append").saveAsTable(BRONZE_TABLE)

print(f"Bronze ingestion complete → {BRONZE_TABLE}")
