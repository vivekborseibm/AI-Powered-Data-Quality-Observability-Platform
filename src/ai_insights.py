"""
ai_insights.py — LLM-powered plain-English quality summaries.

Workflow:
  1. Pull the most recent quality_metrics rows from Delta
  2. Build a structured prompt (using the template in prompts/)
  3. Call the configured LLM API
  4. Parse the JSON response into: summary, root_cause, suggested_action, severity
  5. Write the result to the ai_insights Delta table
"""

from __future__ import annotations

import datetime
import json
import os
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from config import (
    LLM_PROVIDER, LLM_MODEL, LLM_MAX_TOKENS,
    PROMPT_FILE, AI_INSIGHTS_TABLE,
)


# ── Prompt builder ─────────────────────────────────────────────────────────────

def _build_prompt(metrics_summary: str, sample_failures: str) -> str:
    """Load the prompt template and inject runtime values."""
    template = Path(PROMPT_FILE).read_text(encoding="utf-8")
    return (
        template
        .replace("{{METRICS_SUMMARY}}", metrics_summary)
        .replace("{{SAMPLE_FAILURES}}", sample_failures)
    )


# ── LLM call ──────────────────────────────────────────────────────────────────

def _call_llm(prompt: str) -> dict:
    """
    Call the configured LLM and return a parsed dict with keys:
      summary, root_cause, suggested_action, severity
    """
    if LLM_PROVIDER == "openai":
        import openai
        client = openai.OpenAI(api_key=os.environ["OPENAI_API_KEY"])
        response = client.chat.completions.create(
            model=LLM_MODEL,
            max_tokens=LLM_MAX_TOKENS,
            messages=[
                {"role": "system", "content": "You are a data quality analyst. Always respond with valid JSON only."},
                {"role": "user",   "content": prompt},
            ],
        )
        raw = response.choices[0].message.content

    elif LLM_PROVIDER == "anthropic":
        import anthropic
        client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
        response = client.messages.create(
            model=LLM_MODEL,
            max_tokens=LLM_MAX_TOKENS,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = response.content[0].text

    elif LLM_PROVIDER == "watsonx":
        from ibm_watsonx_ai.foundation_models import Model
        model = Model(
            model_id=LLM_MODEL,
            credentials={"apikey": os.environ["WATSONX_API_KEY"], "url": os.environ["WATSONX_URL"]},
            project_id=os.environ["WATSONX_PROJECT_ID"],
        )
        raw = model.generate_text(prompt=prompt)

    else:
        raise ValueError(f"Unknown LLM_PROVIDER: {LLM_PROVIDER}")

    return json.loads(raw)


# ── Main entry point ───────────────────────────────────────────────────────────

def generate_ai_insight(
    spark: SparkSession,
    metrics_df: DataFrame,
    insights_table: str,
    top_n_failures: int = 10,
) -> None:
    """
    Generate an AI insight from the latest quality metrics and write to Delta.
    """
    # Summarize metrics as a compact string
    latest = (
        metrics_df
        .orderBy(F.col("run_timestamp").desc())
        .limit(100)
    )
    metrics_summary = "\n".join(
        f"{r.layer} | {r.table_name} | {r.check_name} | {r.column_name} | "
        f"value={r.metric_value} threshold={r.threshold} passed={r.passed}"
        for r in latest.collect()
    )

    # Sample failing rows for context
    failures = latest.filter(F.col("passed") == False).limit(top_n_failures)  # noqa: E712
    sample_failures = "\n".join(
        f"{r.table_name}.{r.column_name}: {r.check_name}={r.metric_value} ({r.details})"
        for r in failures.collect()
    )

    prompt = _build_prompt(metrics_summary, sample_failures)
    result = _call_llm(prompt)

    # Build insight row
    insight_row = {
        "run_timestamp":    datetime.datetime.utcnow().isoformat(),
        "summary":          result.get("summary", ""),
        "root_cause":       result.get("root_cause", ""),
        "suggested_action": result.get("suggested_action", ""),
        "severity":         result.get("severity", "unknown"),
        "raw_response":     json.dumps(result),
    }

    insight_df = spark.createDataFrame([insight_row])
    insight_df.write.format("delta").mode("append").saveAsTable(insights_table)
