# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer — PostgreSQL AACT Ingestion with Incremental Load and CDC
# MAGIC Reads selected source tables from the AACT PostgreSQL database using JDBC.
# MAGIC
# MAGIC Loading strategy:
# MAGIC - Watermark incremental load where a reliable timestamp column exists
# MAGIC - Full extract + hash-based Delta MERGE fallback otherwise
# MAGIC
# MAGIC Databricks Free Edition note:
# MAGIC - do not hardcode the password
# MAGIC - enter credentials via notebook widgets at runtime

# COMMAND ----------

import sys
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, current_timestamp, lit, md5, coalesce
from pyspark.sql.utils import AnalysisException

sys.path.insert(0, "../src")
from config import (
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_DATABASE,
    POSTGRES_SCHEMA,
    POSTGRES_DEFAULT_USER,
    POSTGRES_JDBC_DRIVER,
    POSTGRES_WIDGET_USER,
    POSTGRES_WIDGET_PASSWORD,
    POSTGRES_TABLES,
    BRONZE_TABLES,
    TABLE_PRIMARY_KEYS,
    TABLE_WATERMARK_COLUMNS,
)

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# Databricks runtime credential entry
dbutils.widgets.text(POSTGRES_WIDGET_USER, POSTGRES_DEFAULT_USER)
dbutils.widgets.text(POSTGRES_WIDGET_PASSWORD, "")

pg_user = dbutils.widgets.get(POSTGRES_WIDGET_USER).strip()
pg_password = dbutils.widgets.get(POSTGRES_WIDGET_PASSWORD)

if not pg_user:
    raise ValueError("PostgreSQL username is empty. Provide it in the notebook widget.")
if not pg_password:
    raise ValueError("PostgreSQL password is empty. Provide it in the notebook widget.")

jdbc_url = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DATABASE}"

jdbc_reader = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .option("driver", POSTGRES_JDBC_DRIVER)
    .option("user", pg_user)
    .option("password", pg_password)
    .option("fetchsize", "10000")
)

# COMMAND ----------

def build_row_hash(df):
    data_columns = [c for c in df.columns if not c.startswith("_")]
    return df.withColumn(
        "_row_hash",
        md5(concat_ws("||", *[coalesce(col(c).cast("string"), lit("<NULL>")) for c in data_columns]))
    )


def table_exists(table_name: str) -> bool:
    try:
        spark.read.table(table_name).limit(1).collect()
        return True
    except AnalysisException:
        return False


def get_last_watermark(bronze_table: str, watermark_column: str):
    if not table_exists(bronze_table):
        return None
    row = spark.sql(f"SELECT MAX({watermark_column}) AS max_watermark FROM {bronze_table}").collect()[0]
    return row["max_watermark"]


def get_source_df(source_fqn: str, watermark_column: str | None, bronze_table: str):
    if watermark_column:
        last_watermark = get_last_watermark(bronze_table, watermark_column)
        if last_watermark is None:
            dbtable_value = source_fqn
            print(f"Initial full load for watermark table {source_fqn}")
        else:
            dbtable_value = f"(SELECT * FROM {source_fqn} WHERE {watermark_column} > '{last_watermark}') AS src"
            print(f"Incremental watermark load for {source_fqn} where {watermark_column} > {last_watermark}")
    else:
        dbtable_value = source_fqn
        print(f"Full extract for CDC/hash merge table {source_fqn}")

    return jdbc_reader.option("dbtable", dbtable_value).load()


def initial_write(df, bronze_table: str):
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(bronze_table)
    )


def merge_into_bronze(source_df, bronze_table: str, primary_keys: list[str]):
    merge_condition = " AND ".join([f"t.{k} = s.{k}" for k in primary_keys])
    update_condition = "t._row_hash <> s._row_hash OR t._is_deleted <> false"

    delta_table = DeltaTable.forName(spark, bronze_table)
    (
        delta_table.alias("t")
        .merge(source_df.alias("s"), merge_condition)
        .whenMatchedUpdateAll(condition=update_condition)
        .whenNotMatchedInsertAll()
        .execute()
    )


def mark_deletes_for_full_extract(source_df, bronze_table: str, primary_keys: list[str]):
    source_keys = source_df.select(*primary_keys).dropDuplicates()
    target_df = spark.read.table(bronze_table)
    deleted_keys = target_df.select(*primary_keys).dropDuplicates().join(source_keys, on=primary_keys, how="left_anti")

    if deleted_keys.count() == 0:
        return

    delta_table = DeltaTable.forName(spark, bronze_table)
    delete_condition = " AND ".join([f"t.{k} = d.{k}" for k in primary_keys])
    (
        delta_table.alias("t")
        .merge(deleted_keys.alias("d"), delete_condition)
        .whenMatchedUpdate(set={
            "_is_deleted": lit(True),
            "_deleted_at": current_timestamp(),
        })
        .execute()
    )

# COMMAND ----------

for source_table in POSTGRES_TABLES:
    source_fqn = f"{POSTGRES_SCHEMA}.{source_table}"
    bronze_table = BRONZE_TABLES[source_table]
    primary_keys = TABLE_PRIMARY_KEYS[source_table]
    watermark_column = TABLE_WATERMARK_COLUMNS[source_table]

    source_df = get_source_df(source_fqn, watermark_column, bronze_table)

    if source_df.rdd.isEmpty() and table_exists(bronze_table):
        print(f"No new rows found for {source_fqn}")
        continue

    source_df = (
        source_df
        .withColumn("_source_system", lit("postgresql_aact"))
        .withColumn("_source_schema", lit(POSTGRES_SCHEMA))
        .withColumn("_source_table", lit(source_table))
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_is_deleted", lit(False))
        .withColumn("_deleted_at", lit(None).cast("timestamp"))
    )
    source_df = build_row_hash(source_df)

    if not table_exists(bronze_table):
        initial_write(source_df, bronze_table)
        print(f"Initial Bronze load complete → {source_fqn} → {bronze_table}")
        continue

    merge_into_bronze(source_df, bronze_table, primary_keys)

    if watermark_column is None:
        mark_deletes_for_full_extract(source_df, bronze_table, primary_keys)

    print(f"Bronze merge complete → {source_fqn} → {bronze_table}")
