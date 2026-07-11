# AI-Powered Data Quality & Observability Platform

A production-style data pipeline built on **Databricks Free Edition** with an AI layer that explains data quality issues in plain English.

This implementation is designed around **real clinical trial data from the AACT PostgreSQL source**, not synthetic CSVs as the main path. Synthetic data generation remains in the repo only as an optional fallback demo path.

---

## Architecture

```text
AACT PostgreSQL source
        ↓
Databricks Free Edition
  Bronze  — JDBC ingestion + incremental / CDC metadata
        ↓
  Silver  — active-record filtering, standardization, validation
        ↓
  Gold    — clinical trial aggregates for reporting
        ↓
DataQualityChecker (PySpark + Delta)
        ↓
quality_metrics Delta table
        ↓
AI Insights (LLM → JSON summary)
        ↓
    ↙           ↘
Dashboard     Alerts stretch path
```

---

## Project Structure

```text
project/
├── notebooks/
│   ├── 01_bronze_ingest.py       # PostgreSQL JDBC ingestion with incremental / CDC logic
│   ├── 02_silver_transform.py    # CDC-aware Silver transforms for AACT tables
│   ├── 03_gold_aggregate.py      # Clinical-trial aggregates → Gold tables
│   └── 04_quality_checks.py      # Run DQ checks + AI insight generation
├── src/
│   ├── quality_checker.py        # Reusable CDC-aware DataQualityChecker
│   ├── ai_insights.py            # LLM summary generation + Delta write
│   └── config.py                 # Source config, table names, thresholds, keys
├── datasets/
│   └── generate_synthetic_data.py   # Optional fallback demo path using Faker
├── tests/
│   └── test_quality_checker.py      # pytest unit tests for CDC-aware checker
├── dashboards/
│   └── quality_dashboard.json       # Databricks SQL dashboard definition
├── docs/
│   ├── architecture.png             # replace placeholder with exported diagram
│   └── demo.gif                     # replace placeholder with screen recording
├── prompts/
│   └── quality_insight_prompt.md    # Prompt template for AI insights
└── README.md
```

---

## Source System

Current PostgreSQL source:

- Host: `aact-db.ctti-clinicaltrials.org`
- Port: `5432`
- Database: `aact`
- Schema: `ctgov`
- Username default: `vivekborse31`
- Password: entered at notebook runtime, never stored in code

### Tables in scope

- `studies`
- `sponsors`
- `conditions`
- `interventions`
- `eligibilities`
- `facilities`
- `outcomes`
- `outcome_analyses`

---

## Secure Connection Handling for Databricks Free Edition

Databricks Free Edition may not support secret scopes the same way as paid tiers.

So this project uses a **Free Edition-friendly approach**:

- keep non-sensitive connection values in `src/config.py`
- enter PostgreSQL username and password through **Databricks notebook widgets** at runtime
- never commit passwords, LLM API keys, or tokens to git

Bronze notebook widget inputs:

- `pg_user`
- `pg_password`

---

## Bronze Loading Strategy

The Bronze layer supports **incremental loading and CDC-style handling**.

### Strategy used

- **Watermark-based incremental load** where a reliable source timestamp exists
- **Full extract + Delta MERGE + row hash comparison** where no trustworthy watermark exists

### Current configuration

- `studies`
  - watermark column: `last_update_posted_date`
- all other scoped tables
  - full extract + hash-based merge fallback

### Bronze metadata columns added

- `_source_system`
- `_source_schema`
- `_source_table`
- `_ingested_at`
- `_row_hash`
- `_is_deleted`
- `_deleted_at`

This lets the project detect inserts, updates, and soft deletes in a Databricks-friendly way.

---

## Silver Layer Behavior

The Silver layer is **CDC-aware**.

It:
- excludes soft-deleted Bronze rows by default
- standardizes and trims source fields
- preserves useful lineage and CDC columns for debugging and observability

This makes downstream quality checks and aggregates reflect the **active state** of the data rather than historical deleted rows.

---

## Quick Start

### 1. Attach PostgreSQL JDBC driver to your Databricks cluster

Recommended Maven package:

```text
org.postgresql:postgresql:42.7.4
```

### 2. Run Bronze notebook

Open `notebooks/01_bronze_ingest.py` in Databricks.

When prompted by widgets, enter:

- PostgreSQL username
- PostgreSQL password

The notebook ingests these AACT source tables into Bronze Delta tables:

- `dq_project.bronze_studies`
- `dq_project.bronze_sponsors`
- `dq_project.bronze_conditions`
- `dq_project.bronze_interventions`
- `dq_project.bronze_eligibilities`
- `dq_project.bronze_facilities`
- `dq_project.bronze_outcomes`
- `dq_project.bronze_outcome_analyses`

### 3. Run Silver notebook

Run:

```text
notebooks/02_silver_transform.py
```

This creates:

- `dq_project.silver_studies`
- `dq_project.silver_sponsors`
- `dq_project.silver_conditions`
- `dq_project.silver_interventions`
- `dq_project.silver_eligibilities`
- `dq_project.silver_facilities`
- `dq_project.silver_outcomes`
- `dq_project.silver_outcome_analyses`

### 4. Run Gold notebook

Run:

```text
notebooks/03_gold_aggregate.py
```

This creates:

- `dq_project.gold_study_status_counts`
- `dq_project.gold_phase_summary`
- `dq_project.gold_condition_intervention_summary`

### 5. Run quality checks and AI insights

Run:

```text
notebooks/04_quality_checks.py
```

This writes to:

- `dq_project.quality_metrics`
- `dq_project.ai_insights`

---

## Quality Checks Implemented

| Check | What it measures |
|---|---|
| `null_rate` | percent of null values per column |
| `duplicate_rate` | percent of duplicate rows on business keys |
| `schema_conformity` | expected column presence and datatype match |
| `outlier_rate` | percent of numeric values beyond z-score threshold |
| `freshness_hours` | age of the most recent active record |
| `referential_integrity` | percent of FK values with no match in the reference table |

### AACT-specific integrity checks currently wired

- `sponsors.nct_id -> studies.nct_id`
- `conditions.nct_id -> studies.nct_id`
- `interventions.nct_id -> studies.nct_id`
- `eligibilities.nct_id -> studies.nct_id`
- `facilities.nct_id -> studies.nct_id`
- `outcomes.nct_id -> studies.nct_id`
- `outcome_analyses.outcome_id -> outcomes.id`

---

## AI Insights

Each pipeline run sends the **latest quality run** to an LLM using `prompts/quality_insight_prompt.md`.

The AI layer summarizes:
- pass/fail counts
- top failing checks
- likely root cause
- suggested next action
- severity

Expected JSON output:

```json
{
  "summary": "Plain-English description for non-technical stakeholders.",
  "root_cause": "Most likely technical cause of the top failure.",
  "suggested_action": "One concrete next step for the data team.",
  "severity": "critical | high | medium | low"
}
```

Additional fields stored in `dq_project.ai_insights`:
- `metrics_run_timestamp`
- `failed_check_count`
- `passed_check_count`
- `raw_response`

### LLM credentials

Do not store API keys in source control.

Set them via environment variables in Databricks or your local execution environment.

Examples:

```bash
# OpenAI
OPENAI_API_KEY=...

# Anthropic
ANTHROPIC_API_KEY=...

# IBM watsonx
WATSONX_API_KEY=...
WATSONX_URL=...
WATSONX_PROJECT_ID=...
```

---

## Dashboard

Dashboard definition file:

- `dashboards/quality_dashboard.json`

The dashboard currently includes:
- overall quality score latest run
- failed checks latest run
- quality trend by layer
- latest quality score by table
- latest failing checks table
- referential integrity failures
- null rate hotspots
- latest AI insight
- AI severity trend

### Databricks SQL dashboard import checklist

1. Open **Databricks SQL**
2. Create or import queries using the SQL from `dashboards/quality_dashboard.json`
3. Point queries to:
   - `dq_project.quality_metrics`
   - `dq_project.ai_insights`
4. Add visualizations matching the widget types in the JSON file
5. Save screenshots for the README / docs

---

## Local Testing

Run locally with:

```bash
pip install pytest pyspark
pytest tests/test_quality_checker.py -v
```

### Notes

- tests are updated for the CDC-aware quality checker
- local IDEs may show unresolved imports for `pyspark` or `delta` if not installed locally
- Databricks runtime is still the target execution environment for the notebooks

---

## Docs and Demo Checklist

Before final submission, replace placeholders and capture these assets.

### `docs/architecture.png`
Include:
- PostgreSQL AACT source
- Bronze incremental / CDC ingestion
- Silver active-record transform
- Gold aggregates
- quality metrics table
- AI insights table
- dashboard

### `docs/demo.gif`
Recommended demo flow:
1. open Bronze notebook and show widget-based credential entry
2. run Bronze ingestion and show Bronze Delta tables
3. run Silver and show active filtered records
4. run Gold and show aggregate tables
5. run quality checks and show `dq_project.quality_metrics`
6. show AI insight row in `dq_project.ai_insights`
7. open the dashboard and show failing checks / AI summary

### Suggested screenshots for README or presentation
- Bronze table preview
- Silver table preview with CDC columns
- Gold aggregate preview
- quality metrics sample rows
- AI insight sample row
- dashboard with multiple widgets visible

---

## Optional Fallback Demo Path

If PostgreSQL access is unavailable during a demo, you can still use:

- `datasets/generate_synthetic_data.py`

This is now a backup path, not the main implementation.

---

## Stretch / Future Improvements

- tighter expected schema definitions for each AACT table
- richer Gold business metrics for storytelling
- alerting when quality score drops below threshold
- CI workflow for linting and tests
- stronger LLM credential handling in production environments
- export final architecture diagram and demo recording into `docs/`
