# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer — AACT Standardization and Validation with CDC Awareness
# MAGIC Reads Bronze AACT tables, excludes soft-deleted Bronze rows,
# MAGIC applies type cleanup and light standardization,
# MAGIC and writes one Silver Delta table per source table.

# COMMAND ----------

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, trim, to_timestamp

sys.path.insert(0, "../src")
from config import BRONZE_TABLES, SILVER_TABLES

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

def active_bronze(table_name: str):
    """Return only active Bronze rows for a given source table."""
    return (
        spark.read.format("delta").table(BRONZE_TABLES[table_name])
        .filter(~col("_is_deleted"))
    )


def add_silver_metadata(df):
    """Preserve helpful lineage / CDC metadata in Silver."""
    metadata_cols = [
        "_source_system",
        "_source_schema",
        "_source_table",
        "_ingested_at",
        "_row_hash",
        "_is_deleted",
        "_deleted_at",
    ]
    available_metadata = [c for c in metadata_cols if c in df.columns]
    business_cols = [c for c in df.columns if c not in available_metadata]
    return df.select(*business_cols, *available_metadata)

# COMMAND ----------

# ── Studies ────────────────────────────────────────────────────────────────────
studies_bz = active_bronze("studies")
studies_sl = add_silver_metadata(
    studies_bz
    .dropDuplicates(["nct_id"])
    .withColumn("brief_title", trim(col("brief_title")))
    .withColumn("official_title", trim(col("official_title")))
    .withColumn("overall_status", lower(trim(col("overall_status"))))
    .withColumn("phase", trim(col("phase")))
    .withColumn("study_type", lower(trim(col("study_type"))))
    .withColumn("last_update_posted_date", to_timestamp(col("last_update_posted_date")))
    .filter(col("nct_id").isNotNull())
)
studies_sl.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(SILVER_TABLES["studies"])
print(f"Silver transform complete → {SILVER_TABLES['studies']}")

# ── Sponsors ───────────────────────────────────────────────────────────────────
sponsors_bz = active_bronze("sponsors")
sponsors_sl = add_silver_metadata(
    sponsors_bz
    .dropDuplicates(["nct_id", "name", "lead_or_collaborator"])
    .withColumn("name", trim(col("name")))
    .withColumn("lead_or_collaborator", lower(trim(col("lead_or_collaborator"))))
    .filter(col("nct_id").isNotNull())
)
sponsors_sl.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(SILVER_TABLES["sponsors"])
print(f"Silver transform complete → {SILVER_TABLES['sponsors']}")

# ── Conditions ─────────────────────────────────────────────────────────────────
conditions_bz = active_bronze("conditions")
conditions_sl = add_silver_metadata(
    conditions_bz
    .dropDuplicates(["nct_id", "name"])
    .withColumn("name", trim(col("name")))
    .filter(col("nct_id").isNotNull())
)
conditions_sl.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(SILVER_TABLES["conditions"])
print(f"Silver transform complete → {SILVER_TABLES['conditions']}")

# ── Interventions ──────────────────────────────────────────────────────────────
interventions_bz = active_bronze("interventions")
interventions_sl = add_silver_metadata(
    interventions_bz
    .dropDuplicates(["nct_id", "intervention_type", "name"])
    .withColumn("intervention_type", lower(trim(col("intervention_type"))))
    .withColumn("name", trim(col("name")))
    .filter(col("nct_id").isNotNull())
)
interventions_sl.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(SILVER_TABLES["interventions"])
print(f"Silver transform complete → {SILVER_TABLES['interventions']}")

# ── Eligibilities ──────────────────────────────────────────────────────────────
eligibilities_bz = active_bronze("eligibilities")
eligibilities_sl = add_silver_metadata(
    eligibilities_bz
    .dropDuplicates(["nct_id"])
    .withColumn("gender", lower(trim(col("gender"))))
    .withColumn("minimum_age", trim(col("minimum_age")))
    .withColumn("maximum_age", trim(col("maximum_age")))
    .filter(col("nct_id").isNotNull())
)
eligibilities_sl.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(SILVER_TABLES["eligibilities"])
print(f"Silver transform complete → {SILVER_TABLES['eligibilities']}")

# ── Facilities ─────────────────────────────────────────────────────────────────
facilities_bz = active_bronze("facilities")
facilities_sl = add_silver_metadata(
    facilities_bz
    .dropDuplicates(["nct_id", "name", "city", "country"])
    .withColumn("name", trim(col("name")))
    .withColumn("city", trim(col("city")))
    .withColumn("state", trim(col("state")))
    .withColumn("country", trim(col("country")))
    .filter(col("nct_id").isNotNull())
)
facilities_sl.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(SILVER_TABLES["facilities"])
print(f"Silver transform complete → {SILVER_TABLES['facilities']}")

# ── Outcomes ───────────────────────────────────────────────────────────────────
outcomes_bz = active_bronze("outcomes")
outcomes_sl = add_silver_metadata(
    outcomes_bz
    .dropDuplicates(["id"])
    .withColumn("outcome_type", lower(trim(col("outcome_type"))))
    .withColumn("title", trim(col("title")))
    .filter(col("nct_id").isNotNull())
)
outcomes_sl.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(SILVER_TABLES["outcomes"])
print(f"Silver transform complete → {SILVER_TABLES['outcomes']}")

# ── Outcome analyses ───────────────────────────────────────────────────────────
outcome_analyses_bz = active_bronze("outcome_analyses")
outcome_analyses_sl = add_silver_metadata(
    outcome_analyses_bz
    .dropDuplicates(["id"])
    .filter(col("outcome_id").isNotNull())
)
outcome_analyses_sl.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(SILVER_TABLES["outcome_analyses"])
print(f"Silver transform complete → {SILVER_TABLES['outcome_analyses']}")
