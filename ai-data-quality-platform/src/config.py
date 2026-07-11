"""
config.py — Central configuration for the Data Quality project.
All table names, paths, and thresholds are defined here.
Never hardcode secrets in notebooks or other src files.
"""

# ── Source system: PostgreSQL AACT ─────────────────────────────────────────────
POSTGRES_HOST = "aact-db.ctti-clinicaltrials.org"
POSTGRES_PORT = 5432
POSTGRES_DATABASE = "aact"
POSTGRES_SCHEMA = "ctgov"
POSTGRES_DEFAULT_USER = "vivekborse31"
POSTGRES_JDBC_DRIVER = "org.postgresql.Driver"

# Databricks Free-friendly credential handling
# Keep password out of source control. Enter it at notebook runtime via widgets.
POSTGRES_WIDGET_USER = "pg_user"
POSTGRES_WIDGET_PASSWORD = "pg_password"

# Source tables selected for Bronze ingestion
POSTGRES_TABLES = [
    "studies",
    "sponsors",
    "conditions",
    "interventions",
    "eligibilities",
    "facilities",
    "outcomes",
    "outcome_analyses",
]

# Incremental / CDC configuration
# Use watermark-based incremental loading where a reliable source timestamp exists.
# Otherwise fall back to full extract + hash-based merge.
TABLE_PRIMARY_KEYS = {
    "studies": ["nct_id"],
    "sponsors": ["nct_id", "name", "lead_or_collaborator"],
    "conditions": ["nct_id", "name"],
    "interventions": ["nct_id", "intervention_type", "name"],
    "eligibilities": ["nct_id"],
    "facilities": ["nct_id", "name", "city", "country"],
    "outcomes": ["id"],
    "outcome_analyses": ["id"],
}

TABLE_WATERMARK_COLUMNS = {
    "studies": "last_update_posted_date",
    "sponsors": None,
    "conditions": None,
    "interventions": None,
    "eligibilities": None,
    "facilities": None,
    "outcomes": None,
    "outcome_analyses": None,
}

# ── Bronze / Silver / Gold table namespaces ───────────────────────────────────
BRONZE_TABLE_PREFIX = "dq_project.bronze_"
SILVER_TABLE_PREFIX = "dq_project.silver_"

BRONZE_TABLES = {table: f"{BRONZE_TABLE_PREFIX}{table}" for table in POSTGRES_TABLES}
SILVER_TABLES = {table: f"{SILVER_TABLE_PREFIX}{table}" for table in POSTGRES_TABLES}

GOLD_TABLE_STUDY_STATUS = "dq_project.gold_study_status_counts"
GOLD_TABLE_PHASE_SUMMARY = "dq_project.gold_phase_summary"
GOLD_TABLE_CONDITION_INTERVENTION = "dq_project.gold_condition_intervention_summary"

QUALITY_METRICS_TABLE = "dq_project.quality_metrics"
AI_INSIGHTS_TABLE = "dq_project.ai_insights"

# ── Quality thresholds ─────────────────────────────────────────────────────────
NULL_RATE_THRESHOLD = 0.05
DUPLICATE_RATE_THRESHOLD = 0.02
FRESHNESS_MAX_HOURS = 24 * 30
OUTLIER_ZSCORE_THRESHOLD = 3.0
QUALITY_SCORE_ALERT_BELOW = 0.80

# ── Rolling baseline window ────────────────────────────────────────────────────
BASELINE_WINDOW_DAYS = 7

# ── LLM / AI insights ─────────────────────────────────────────────────────────
LLM_PROVIDER = "openai"
LLM_MODEL = "gpt-4o-mini"
LLM_MAX_TOKENS = 512
PROMPT_FILE = "prompts/quality_insight_prompt.md"

# ── Alerts (stretch) ──────────────────────────────────────────────────────────
SLACK_WEBHOOK_URL = ""
ALERT_EMAIL = ""
