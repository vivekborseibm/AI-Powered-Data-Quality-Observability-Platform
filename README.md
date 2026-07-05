# AI-Powered Data Quality & Observability Platform

A production-style data pipeline built on **Databricks Free Edition** with an AI layer that automatically explains data quality issues in plain English.

---

## Architecture

```
Synthetic CSV/JSON (injected defects)
        ↓
Databricks Free Edition
  Bronze  — raw ingestion + metadata
        ↓
  Silver  — deduped, typed, validated
        ↓
  Gold    — daily revenue, customer LTV
        ↓
DataQualityChecker (PySpark + Delta)
        ↓
quality_metrics Delta table
        ↓
AI Insights (LLM → JSON)
        ↓
    ↙           ↘
Dashboard     Alerts (stretch)
```

See [`docs/architecture.png`](docs/architecture.png) for the full diagram.

---

## Project Structure

```
project/
├── notebooks/
│   ├── 01_bronze_ingest.py       # Raw ingestion → Bronze Delta table
│   ├── 02_silver_transform.py    # Dedup, cast types, handle nulls → Silver
│   ├── 03_gold_aggregate.py      # Business aggregates → Gold tables
│   └── 04_quality_checks.py      # Run all checks + AI insight generation
├── src/
│   ├── quality_checker.py        # DataQualityChecker class (reusable)
│   ├── ai_insights.py            # LLM call + prompt builder + Delta write
│   └── config.py                 # All table names, paths, thresholds
├── datasets/
│   └── generate_synthetic_data.py   # Faker script with controllable defects
├── tests/
│   └── test_quality_checker.py      # pytest unit tests (local Spark)
├── dashboards/
│   └── quality_dashboard.json       # Databricks SQL dashboard definition
├── docs/
│   ├── architecture.png
│   └── demo.gif
├── prompts/
│   └── quality_insight_prompt.md    # LLM prompt template
└── README.md
```

---

## Quick Start

### 1. Generate synthetic data (local)

```bash
pip install faker pandas
python datasets/generate_synthetic_data.py \
  --rows 5000 \
  --null-rate 0.08 \
  --duplicate-rate 0.05 \
  --schema-drift
```

Output: `datasets/generated/orders.csv`, `customers.csv`, `products.csv`

### 2. Upload to Databricks

```
dbfs:/FileStore/data_quality_project/raw/
```

Upload all three CSV files there via the Databricks UI or CLI.

### 3. Run notebooks in order

| Notebook | What it does |
|---|---|
| `01_bronze_ingest.py` | Reads CSVs → `dq_project.bronze_orders` |
| `02_silver_transform.py` | Cleans Bronze → `dq_project.silver_orders` |
| `03_gold_aggregate.py` | Aggregates Silver → Gold tables |
| `04_quality_checks.py` | Runs all quality checks + calls LLM |

### 4. View results

- **quality_metrics** table: `SELECT * FROM dq_project.quality_metrics ORDER BY run_timestamp DESC`
- **ai_insights** table: `SELECT * FROM dq_project.ai_insights ORDER BY run_timestamp DESC`
- Import `dashboards/quality_dashboard.json` into Databricks SQL

---

## Configuration

All settings live in [`src/config.py`](src/config.py). Key values:

| Setting | Default | Description |
|---|---|---|
| `NULL_RATE_THRESHOLD` | `0.05` | Flag column if null % > 5% |
| `DUPLICATE_RATE_THRESHOLD` | `0.02` | Flag table if duplicate % > 2% |
| `FRESHNESS_MAX_HOURS` | `24` | Flag if latest record older than 24h |
| `OUTLIER_ZSCORE_THRESHOLD` | `3.0` | Z-score cutoff for numeric outliers |
| `LLM_PROVIDER` | `"openai"` | `"openai"` \| `"anthropic"` \| `"watsonx"` |
| `LLM_MODEL` | `"gpt-4o-mini"` | Model used for quality summaries |

### LLM API Keys

Set as environment variables (never hardcode):

```bash
# OpenAI
export OPENAI_API_KEY=sk-...

# Anthropic
export ANTHROPIC_API_KEY=sk-ant-...

# IBM watsonx
export WATSONX_API_KEY=...
export WATSONX_URL=https://us-south.ml.cloud.ibm.com
export WATSONX_PROJECT_ID=...
```

---

## Running Tests Locally

```bash
pip install pytest pyspark
pytest tests/test_quality_checker.py -v
```

No Databricks cluster required — tests use PySpark local mode.

---

## Quality Checks Implemented

| Check | What it measures |
|---|---|
| `null_rate` | % of null values per column |
| `duplicate_rate` | % of duplicate rows on key columns |
| `schema_conformity` | Column presence + dtype match vs expected schema |
| `outlier_rate` | % of numeric values beyond z-score threshold |
| `freshness_hours` | Age of most recent record in hours |
| `referential_integrity` | % of FK values with no match in reference table |

---

## AI Insights

Each pipeline run sends the latest quality metrics to an LLM using the prompt in [`prompts/quality_insight_prompt.md`](prompts/quality_insight_prompt.md).

The LLM returns structured JSON:

```json
{
  "summary": "Plain-English description for non-technical stakeholders.",
  "root_cause": "Most likely technical cause of the top failure.",
  "suggested_action": "One concrete next step for the data team.",
  "severity": "critical | high | medium | low"
}
```

Results are stored in `dq_project.ai_insights` and surfaced on the dashboard.

---

## Demo

See [`docs/demo.gif`](docs/demo.gif) for a walkthrough of:
1. Generating data with injected defects
2. Running the full pipeline
3. Quality metrics populating
4. AI insight explaining the root cause
5. Dashboard updating

---

## Stretch Goals (not yet implemented)

- **dbt models** layered on Silver/Gold
- **Slack/email alerts** when quality score < 80%
- **GitHub Actions CI** — lint + pytest on push
- **Streamlit NL-query box** — ask questions about metrics in plain English
