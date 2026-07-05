"""
config.py — Central configuration for the Data Quality project.
All table names, paths, and thresholds are defined here.
Never hardcode these values in notebooks or other src files.
"""

# ── Data paths ─────────────────────────────────────────────────────────────────
RAW_DATA_PATH = "dbfs:/FileStore/data_quality_project/raw/"

# ── Delta table names ──────────────────────────────────────────────────────────
BRONZE_TABLE              = "dq_project.bronze_orders"
SILVER_TABLE              = "dq_project.silver_orders"
GOLD_TABLE_DAILY_REVENUE  = "dq_project.gold_daily_revenue"
GOLD_TABLE_CUSTOMER_LTV   = "dq_project.gold_customer_ltv"
QUALITY_METRICS_TABLE     = "dq_project.quality_metrics"
AI_INSIGHTS_TABLE         = "dq_project.ai_insights"

# ── Quality thresholds ─────────────────────────────────────────────────────────
NULL_RATE_THRESHOLD        = 0.05   # flag if null % > 5 %
DUPLICATE_RATE_THRESHOLD   = 0.02   # flag if duplicate % > 2 %
FRESHNESS_MAX_HOURS        = 24     # flag if latest record is older than 24 h
OUTLIER_ZSCORE_THRESHOLD   = 3.0    # flag if z-score > 3 for numeric columns
QUALITY_SCORE_ALERT_BELOW  = 0.80   # trigger alert if overall score < 80 %

# ── Rolling baseline window ────────────────────────────────────────────────────
BASELINE_WINDOW_DAYS = 7            # compare metrics against last 7-day average

# ── LLM / AI insights ─────────────────────────────────────────────────────────
LLM_PROVIDER   = "openai"           # options: "openai" | "anthropic" | "watsonx"
LLM_MODEL      = "gpt-4o-mini"      # cheap + fast summarization model
LLM_MAX_TOKENS = 512
PROMPT_FILE    = "prompts/quality_insight_prompt.md"

# ── Alerts (stretch) ──────────────────────────────────────────────────────────
SLACK_WEBHOOK_URL = ""              # set via environment variable in production
ALERT_EMAIL       = ""              # set via environment variable in production
