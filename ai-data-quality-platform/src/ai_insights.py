"""
ai_insights.py — LLM-powered plain-English quality summaries.

Workflow:
  1. Pull the most recent quality_metrics rows from Delta
  2. Build a structured prompt using the template in prompts/
  3. Call the configured LLM API
  4. Parse the JSON response into summary, root_cause, suggested_action, severity
  5. Write the result to the ai_insights Delta table

AACT/CDC-aware behavior:
  - summarize the latest quality run across Bronze, Silver, and Gold layers
  - highlight failed checks first
  - include layer, table, check, metric, threshold, and details in the prompt context
"""

from __future__ import annotations

import datetime
import json
import os
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from config import LLM_PROVIDER, LLM_MODEL, LLM_MAX_TOKENS, PROMPT_FILE


def _build_prompt(metrics_summary: str, sample_failures: str) -> str:
    """Load the prompt template and inject runtime values."""
    template = Path(PROMPT_FILE).read_text(encoding="utf-8")
    return (
        template
        .replace("{{METRICS_SUMMARY}}", metrics_summary)
        .replace("{{SAMPLE_FAILURES}}", sample_failures)
    )


def _parse_json_response(raw: str) -> dict:
    """Best-effort parse for providers that may wrap JSON in code fences."""
    cleaned = raw.strip()
    if cleaned.startswith("```"):
        cleaned = cleaned.strip("`")
        if cleaned.startswith("json"):
            cleaned = cleaned[4:].strip()
    return json.loads(cleaned)


def _call_llm(prompt: str) -> dict:
    """Call the configured LLM and return parsed JSON."""
    if LLM_PROVIDER == "openai":
        import openai

        client = openai.OpenAI(api_key=os.environ["OPENAI_API_KEY"])
        response = client.chat.completions.create(
            model=LLM_MODEL,
            max_tokens=LLM_MAX_TOKENS,
            messages=[
                {
                    "role": "system",
                    "content": "You are a senior data quality analyst for clinical trial datasets. Always respond with valid JSON only.",
                },
                {"role": "user", "content": prompt},
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
            credentials={
                "apikey": os.environ["WATSONX_API_KEY"],
                "url": os.environ["WATSONX_URL"],
            },
            project_id=os.environ["WATSONX_PROJECT_ID"],
        )
        raw = model.generate_text(prompt=prompt)

    else:
        raise ValueError(f"Unknown LLM_PROVIDER: {LLM_PROVIDER}")

    return _parse_json_response(raw)


def generate_ai_insight(
    spark: SparkSession,
    metrics_df: DataFrame,
    insights_table: str,
    top_n_failures: int = 12,
    max_metrics: int = 150,
) -> None:
    """Generate an AI insight from the latest quality metrics and write to Delta."""

    latest_run = metrics_df.select(F.max("run_timestamp").alias("max_run_timestamp")).collect()[0]["max_run_timestamp"]
    if latest_run is None:
        print("No quality metrics available yet. Skipping AI insight generation.")
        return

    latest = (
        metrics_df
        .filter(F.col("run_timestamp") == latest_run)
        .orderBy(F.col("passed").asc(), F.col("layer").asc(), F.col("table_name").asc(), F.col("check_name").asc())
        .limit(max_metrics)
    )

    latest_rows = latest.collect()
    fail_count = sum(1 for r in latest_rows if not r.passed)
    pass_count = sum(1 for r in latest_rows if r.passed)

    headline = (
        f"Latest run timestamp: {latest_run}\n"
        f"Total checks: {len(latest_rows)} | Passed: {pass_count} | Failed: {fail_count}\n"
        f"Dataset domain: AACT clinical trials with CDC-aware Bronze and Silver layers"
    )

    metrics_summary = headline + "\n" + "\n".join(
        f"layer={r.layer} | table={r.table_name} | check={r.check_name} | column={r.column_name} | "
        f"value={r.metric_value} | threshold={r.threshold} | passed={r.passed} | details={r.details}"
        for r in latest_rows
    )

    failures = [r for r in latest_rows if not r.passed][:top_n_failures]
    sample_failures = "\n".join(
        f"layer={r.layer} | table={r.table_name} | column={r.column_name} | "
        f"check={r.check_name} | value={r.metric_value} | threshold={r.threshold} | details={r.details}"
        for r in failures
    )
    if not sample_failures:
        sample_failures = "No failing checks in the latest run."

    prompt = _build_prompt(metrics_summary, sample_failures)
    result = _call_llm(prompt)

    insight_row = {
        "run_timestamp": datetime.datetime.utcnow().isoformat(),
        "metrics_run_timestamp": str(latest_run),
        "summary": result.get("summary", ""),
        "root_cause": result.get("root_cause", ""),
        "suggested_action": result.get("suggested_action", ""),
        "severity": result.get("severity", "unknown"),
        "failed_check_count": int(fail_count),
        "passed_check_count": int(pass_count),
        "raw_response": json.dumps(result),
    }

    insight_df = spark.createDataFrame([insight_row])
    insight_df.write.format("delta").mode("append").saveAsTable(insights_table)
