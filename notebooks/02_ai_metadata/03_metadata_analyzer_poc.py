# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — AI Metadata Analyzer
# MAGIC
# MAGIC Production flow:
# MAGIC
# MAGIC ```
# MAGIC Metadata Table -> Read Target Table(s) -> Generate Prompt -> Gemini
# MAGIC   -> Validate JSON -> Generate Markdown -> Persist -> Display HTML Summary
# MAGIC ```
# MAGIC
# MAGIC Reads table metadata from `<metadata_catalog>.metadata.table_metadata` (written by
# MAGIC `02_metadata_collector.py`), asks Gemini for a strict-JSON analysis via the shared
# MAGIC client, validates and renders it, and persists one record per analyzed table into
# MAGIC `<metadata_catalog>.metadata.ai_analysis` (created automatically if it doesn't exist).
# MAGIC
# MAGIC **Table selection** (`target_table_name` widget):
# MAGIC - a specific name (e.g. `customer`) — analyze only that table
# MAGIC - `ALL` (case-insensitive) — analyze every distinct table_name present in the
# MAGIC   metadata repository, sequentially; a failure on one table is recorded with
# MAGIC   `status='FAILED'` and does not stop the rest
# MAGIC
# MAGIC **Note:** the metadata sent to Gemini includes real column sample values. That's
# MAGIC acceptable here because AdventureWorks is synthetic demo data with no real PII —
# MAGIC this would need a redaction/guardrail step before pointing this flow at real data.

# COMMAND ----------

# MAGIC %md ## Import the shared Gemini client
# MAGIC
# MAGIC Brings `generate_content(...)` into scope from `notebooks/00_shared/01_gemini_client.py`.

# COMMAND ----------

# MAGIC %run "../00_shared/01_gemini_client"

# COMMAND ----------

# MAGIC %md ## Widgets (job/notebook parameters)

# COMMAND ----------

dbutils.widgets.text("metadata_catalog", "aide", "Metadata Catalog")
dbutils.widgets.text("target_table_name", "customer", "Target Table Name (or ALL)")

# COMMAND ----------

# MAGIC %md ## Imports & logging

# COMMAND ----------

import json
import logging
import re
import time
from datetime import datetime
from typing import Optional

from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

logger = logging.getLogger("aide.metadata_analyzer")

# COMMAND ----------

# MAGIC %md ## Exceptions

# COMMAND ----------


class MetadataNotFoundError(Exception):
    """Raised when no metadata row exists for the requested table."""


class AIResponseValidationError(Exception):
    """Raised when Gemini's response cannot be parsed as the expected JSON schema."""


# COMMAND ----------

# MAGIC %md ## Helper functions — table discovery & metadata retrieval

# COMMAND ----------


def discover_target_tables(
    spark: SparkSession, metadata_catalog: str, target_table_name: str
) -> list:
    """Resolve the widget value to a concrete list of table_names to analyze.

    `ALL` (case-insensitive) expands to every distinct table_name already present in
    the metadata repository, sorted for a deterministic run order; anything else is
    treated as one specific table name.
    """
    if target_table_name.strip().upper() != "ALL":
        return [target_table_name]

    full_table_name = f"{metadata_catalog}.metadata.table_metadata"
    rows = spark.table(full_table_name).select("table_name").distinct().collect()
    return sorted(row.table_name for row in rows)


def fetch_table_metadata(
    spark: SparkSession, metadata_catalog: str, target_table_name: str
) -> dict:
    """Read the single metadata row for `target_table_name` from the metadata
    repository and return it as a plain (JSON-serializable-ready) nested dict.
    """
    full_table_name = f"{metadata_catalog}.metadata.table_metadata"
    matches = (
        spark.table(full_table_name)
        .filter(F.col("table_name") == target_table_name)
        .collect()
    )

    if not matches:
        raise MetadataNotFoundError(
            f"No metadata found for table_name='{target_table_name}' in '{full_table_name}'. "
            f"Has 02_metadata_collector.py been run for this table?"
        )
    if len(matches) > 1:
        logger.warning(
            "Found %d metadata rows for table_name='%s'; using the first.",
            len(matches),
            target_table_name,
        )

    return matches[0].asDict(recursive=True)


def build_prompt(metadata: dict) -> str:
    """Render the metadata dict as indented JSON and embed it in the analysis prompt.

    Requests strict JSON back (validated by parse_ai_json_response below) — no
    markdown fences, no commentary — because the response is persisted as structured
    data, not printed as free-form text. `default=str` covers the handful of
    non-JSON-native values in the metadata (e.g. created_at/last_modified_at
    timestamps).
    """
    metadata_json = json.dumps(metadata, indent=2, default=str)

    return f"""You are a Senior Data Architect.

Analyze the following table metadata and respond with STRICT JSON only — no markdown
code fences, no commentary before or after, nothing outside the JSON object itself.

Return a single JSON object with exactly these keys:
{{
  "business_purpose": "<string>",
  "table_description": "<string>",
  "column_descriptions": [
    {{ "column_name": "<string>", "description": "<string>" }}
  ],
  "candidate_primary_keys": ["<column_name>", "..."],
  "data_quality_observations": ["<string>", "..."],
  "possible_relationships": ["<string>", "..."],
  "suggested_improvements": ["<string>", "..."],
  "health_score": <integer 0-100, overall data health/quality assessment>,
  "confidence_score": <integer 0-100, your confidence in this analysis>
}}

Only use the supplied metadata. Do not hallucinate columns, values, or relationships
that are not evidenced by the metadata below.

Metadata:
{metadata_json}
"""


# COMMAND ----------

# MAGIC %md ## Helper functions — AI response validation

# COMMAND ----------

_CODE_FENCE_PATTERN = re.compile(r"^```(?:json)?\s*(.*?)\s*```$", re.DOTALL)


def parse_ai_json_response(raw_text: str) -> dict:
    """Parse Gemini's response as the strict JSON requested in the prompt.

    Defensively strips a markdown code fence if the model wrapped the JSON in one
    despite being told not to — a real-world model behavior worth tolerating rather
    than failing on. Anything that still isn't valid JSON after that is a genuine
    failure for this table (see AIResponseValidationError), not free-form text to
    fall back to — the pipeline stores structured JSON, never raw text.
    """
    cleaned = raw_text.strip()
    fence_match = _CODE_FENCE_PATTERN.match(cleaned)
    if fence_match:
        cleaned = fence_match.group(1).strip()

    try:
        return json.loads(cleaned)
    except json.JSONDecodeError as exc:
        raise AIResponseValidationError(f"Gemini response is not valid JSON: {exc}") from exc


def _coerce_score(value) -> Optional[int]:
    """Best-effort coercion of a health/confidence score to a 0-100 int.

    Missing or unparseable scores return None rather than failing the whole
    analysis — the narrative fields are still useful even without a numeric score.
    """
    if value is None:
        return None
    try:
        return max(0, min(100, int(round(float(value)))))
    except (TypeError, ValueError):
        logger.warning("Could not coerce score value %r to int; storing NULL.", value)
        return None


# COMMAND ----------

# MAGIC %md ## Helper functions — markdown rendering
# MAGIC
# MAGIC Pure formatting: turns the already-validated JSON into Markdown. No second AI call.

# COMMAND ----------


def render_markdown(table_name: str, analysis: dict) -> str:
    """Render the parsed analysis JSON as a readable Markdown document."""

    def _bullets(items) -> str:
        items = items or []
        return "\n".join(f"- {item}" for item in items) or "- (none provided)"

    column_rows = "\n".join(
        f"| {c.get('column_name', '')} | {c.get('description', '')} |"
        for c in (analysis.get("column_descriptions") or [])
    ) or "| (none provided) | |"

    health_score = analysis.get("health_score")
    confidence_score = analysis.get("confidence_score")

    return f"""# AI Metadata Analysis — {table_name}

**Health Score:** {health_score if health_score is not None else "N/A"}/100
**Confidence Score:** {confidence_score if confidence_score is not None else "N/A"}/100

## Business Purpose
{analysis.get("business_purpose", "(none provided)")}

## Table Description
{analysis.get("table_description", "(none provided)")}

## Column Descriptions
| Column | Description |
|---|---|
{column_rows}

## Candidate Primary Keys
{_bullets(analysis.get("candidate_primary_keys"))}

## Data Quality Observations
{_bullets(analysis.get("data_quality_observations"))}

## Possible Relationships
{_bullets(analysis.get("possible_relationships"))}

## Suggested Improvements
{_bullets(analysis.get("suggested_improvements"))}
"""


# COMMAND ----------

# MAGIC %md ## Persistence — `ai_analysis` schema & writes

# COMMAND ----------

AI_ANALYSIS_SCHEMA = StructType(
    [
        StructField("catalog_name", StringType(), nullable=False),
        StructField("schema_name", StringType(), nullable=True),
        StructField("table_name", StringType(), nullable=False),
        StructField("analysis_timestamp", TimestampType(), nullable=False),
        StructField("model_name", StringType(), nullable=False),
        StructField("analysis_json", StringType(), nullable=True),
        StructField("analysis_markdown", StringType(), nullable=True),
        StructField("health_score", IntegerType(), nullable=True),
        StructField("confidence_score", IntegerType(), nullable=True),
        StructField("processing_time_ms", LongType(), nullable=False),
        StructField("status", StringType(), nullable=False),
        StructField("error_message", StringType(), nullable=True),
    ]
)


_SPARK_TYPE_TO_SQL = {
    "StringType": "STRING",
    "TimestampType": "TIMESTAMP",
    "IntegerType": "INT",
    "LongType": "BIGINT",
}


def _schema_to_ddl(schema: StructType) -> str:
    """Render a StructType as a `col TYPE, col TYPE, ...` DDL fragment."""
    return ", ".join(
        f"{field.name} {_SPARK_TYPE_TO_SQL[type(field.dataType).__name__]}"
        for field in schema.fields
    )


def ensure_ai_analysis_table(spark: SparkSession, metadata_catalog: str) -> str:
    """Create `<metadata_catalog>.metadata.ai_analysis` if it doesn't already exist.

    Uses `CREATE TABLE IF NOT EXISTS` DDL directly rather than a tableExists() check
    plus a conditional write — atomic, and avoids any ambiguity in how tableExists()
    resolves a fully-qualified three-level table name across Spark versions.
    """
    full_table_name = f"{metadata_catalog}.metadata.ai_analysis"
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {metadata_catalog}.metadata")
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {full_table_name} ({_schema_to_ddl(AI_ANALYSIS_SCHEMA)}) "
        f"USING DELTA"
    )
    return full_table_name


def build_analysis_record(
    catalog_name: Optional[str],
    schema_name: Optional[str],
    table_name: str,
    model_name: str,
    processing_time_ms: int,
    status: str,
    analysis_json_text: Optional[str] = None,
    analysis_markdown: Optional[str] = None,
    health_score: Optional[int] = None,
    confidence_score: Optional[int] = None,
    error_message: Optional[str] = None,
) -> dict:
    """Build one ai_analysis row, for either a SUCCESS or a FAILED table."""
    return {
        "catalog_name": catalog_name,
        "schema_name": schema_name,
        "table_name": table_name,
        "analysis_timestamp": datetime.utcnow(),
        "model_name": model_name,
        "analysis_json": analysis_json_text,
        "analysis_markdown": analysis_markdown,
        "health_score": health_score,
        "confidence_score": confidence_score,
        "processing_time_ms": processing_time_ms,
        "status": status,
        "error_message": error_message,
    }


def persist_analysis_record(spark: SparkSession, full_table_name: str, record: dict) -> None:
    """Append one analysis record. Called once per table, immediately after that
    table finishes processing — so a crash partway through an ALL-mode run loses at
    most the in-flight table, not every table already analyzed.
    """
    spark.createDataFrame([Row(**record)], schema=AI_ANALYSIS_SCHEMA).write.format(
        "delta"
    ).mode("append").saveAsTable(full_table_name)


# COMMAND ----------

# MAGIC %md ## Per-table orchestration
# MAGIC
# MAGIC The one reusable pipeline used for both a single table and every table in ALL
# MAGIC mode — never stops on failure, always returns a record (SUCCESS or FAILED).

# COMMAND ----------


def analyze_one_table(
    spark: SparkSession, metadata_catalog: str, target_table_name: str
) -> dict:
    """Run metadata fetch -> prompt -> Gemini -> validate -> markdown for one table.

    Always returns a record dict ready for persist_analysis_record — never raises;
    any failure along the way is captured as a status='FAILED' record instead.
    """
    start = time.perf_counter()
    catalog_name = metadata_catalog
    schema_name = None

    try:
        metadata = fetch_table_metadata(spark, metadata_catalog, target_table_name)
        catalog_name = metadata.get("catalog_name", metadata_catalog)
        schema_name = metadata.get("schema_name")

        prompt = build_prompt(metadata)
        raw_response = generate_content(prompt)
        analysis = parse_ai_json_response(raw_response)
        markdown = render_markdown(target_table_name, analysis)

        processing_time_ms = int((time.perf_counter() - start) * 1000)
        logger.info("Analyzed '%s' successfully in %d ms.", target_table_name, processing_time_ms)

        return build_analysis_record(
            catalog_name=catalog_name,
            schema_name=schema_name,
            table_name=target_table_name,
            model_name=DEFAULT_MODEL,
            processing_time_ms=processing_time_ms,
            status="SUCCESS",
            analysis_json_text=json.dumps(analysis),
            analysis_markdown=markdown,
            health_score=_coerce_score(analysis.get("health_score")),
            confidence_score=_coerce_score(analysis.get("confidence_score")),
        )

    except Exception as exc:
        processing_time_ms = int((time.perf_counter() - start) * 1000)
        logger.error("Analysis failed for '%s': %s", target_table_name, exc)
        return build_analysis_record(
            catalog_name=catalog_name,
            schema_name=schema_name,
            table_name=target_table_name,
            model_name=DEFAULT_MODEL,
            processing_time_ms=processing_time_ms,
            status="FAILED",
            error_message=str(exc),
        )


# COMMAND ----------

# MAGIC %md ## HTML dashboard rendering
# MAGIC
# MAGIC Pure presentation, isolated from the processing/persistence logic above — takes the
# MAGIC collected records and renders one self-contained HTML string for `displayHTML`.

# COMMAND ----------

_HEALTH_BUCKETS = [
    ("Healthy (80-100)", 80, 100, "#16a34a"),
    ("Needs Review (50-79)", 50, 79, "#d97706"),
    ("Poor (0-49)", 0, 49, "#dc2626"),
]


def _status_badge(status: str) -> str:
    color = "#16a34a" if status == "SUCCESS" else "#dc2626"
    return (
        f'<span style="background:{color};color:#fff;padding:3px 10px;'
        f'border-radius:12px;font-size:12px;font-weight:600;">{status}</span>'
    )


def _health_distribution_html(results: list) -> str:
    scored = [r["health_score"] for r in results if r.get("health_score") is not None]
    total_scored = len(scored) or 1  # avoid division by zero; bars just render at 0%

    rows = []
    for label, low, high, color in _HEALTH_BUCKETS:
        count = sum(1 for s in scored if low <= s <= high)
        pct = round(count / total_scored * 100)
        rows.append(
            f'<div style="margin-bottom:10px;">'
            f'<div style="display:flex;justify-content:space-between;font-size:13px;'
            f'color:#374151;margin-bottom:4px;"><span>{label}</span><span>{count}</span></div>'
            f'<div style="background:#e5e7eb;border-radius:6px;height:10px;overflow:hidden;">'
            f'<div style="background:{color};width:{pct}%;height:100%;"></div></div>'
            f"</div>"
        )
    no_score = sum(1 for r in results if r.get("health_score") is None)
    if no_score:
        rows.append(
            f'<div style="font-size:12px;color:#6b7280;">N/A (no score): {no_score}</div>'
        )
    return "".join(rows)


def render_html_dashboard(results: list, total_elapsed_seconds: float) -> str:
    """Build the enterprise-style HTML summary dashboard for displayHTML()."""
    total = len(results)
    succeeded = sum(1 for r in results if r["status"] == "SUCCESS")
    failed = total - succeeded

    table_rows = "".join(
        f"<tr>"
        f"<td>{r['table_name']}</td>"
        f"<td>{_status_badge(r['status'])}</td>"
        f"<td>{r['health_score'] if r['health_score'] is not None else '—'}</td>"
        f"<td>{r['confidence_score'] if r['confidence_score'] is not None else '—'}</td>"
        f"<td>{r['processing_time_ms']:,} ms</td>"
        f"<td style=\"color:#dc2626;font-size:12px;\">{r['error_message'] or ''}</td>"
        f"</tr>"
        for r in results
    )

    return f"""
<div style="font-family:-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif;
            color:#111827;max-width:1000px;">
  <h2 style="margin-bottom:4px;">AI Metadata Analyzer — Run Summary</h2>
  <p style="color:#6b7280;margin-top:0;">
    Metadata Table &rarr; Prompt Generation &rarr; Gemini &rarr; AI Analysis
  </p>

  <div style="display:flex;gap:16px;margin:20px 0;flex-wrap:wrap;">
    <div style="background:#f3f4f6;border-radius:10px;padding:16px 24px;min-width:140px;">
      <div style="font-size:28px;font-weight:700;">{total}</div>
      <div style="color:#6b7280;font-size:13px;">Total Tables</div>
    </div>
    <div style="background:#ecfdf5;border-radius:10px;padding:16px 24px;min-width:140px;">
      <div style="font-size:28px;font-weight:700;color:#16a34a;">{succeeded}</div>
      <div style="color:#6b7280;font-size:13px;">Successful</div>
    </div>
    <div style="background:#fef2f2;border-radius:10px;padding:16px 24px;min-width:140px;">
      <div style="font-size:28px;font-weight:700;color:#dc2626;">{failed}</div>
      <div style="color:#6b7280;font-size:13px;">Failed</div>
    </div>
    <div style="background:#eff6ff;border-radius:10px;padding:16px 24px;min-width:140px;">
      <div style="font-size:28px;font-weight:700;color:#2563eb;">{total_elapsed_seconds:.1f}s</div>
      <div style="color:#6b7280;font-size:13px;">Execution Time</div>
    </div>
  </div>

  <h3 style="margin-bottom:8px;">Health Score Distribution</h3>
  <div style="max-width:500px;margin-bottom:24px;">
    {_health_distribution_html(results)}
  </div>

  <h3 style="margin-bottom:8px;">Processing Summary</h3>
  <table style="width:100%;border-collapse:collapse;font-size:13px;">
    <thead>
      <tr style="text-align:left;border-bottom:2px solid #e5e7eb;color:#6b7280;">
        <th style="padding:8px 6px;">Table</th>
        <th style="padding:8px 6px;">Status</th>
        <th style="padding:8px 6px;">Health</th>
        <th style="padding:8px 6px;">Confidence</th>
        <th style="padding:8px 6px;">Time</th>
        <th style="padding:8px 6px;">Error</th>
      </tr>
    </thead>
    <tbody>
      {table_rows}
    </tbody>
  </table>
</div>
"""


# COMMAND ----------

# MAGIC %md ## main()

# COMMAND ----------


def main() -> None:
    metadata_catalog = dbutils.widgets.get("metadata_catalog")
    target_table_name = dbutils.widgets.get("target_table_name")

    logger.info(
        "Starting AI metadata analysis | catalog=%s target=%s",
        metadata_catalog,
        target_table_name,
    )

    start_time = time.perf_counter()

    try:
        ai_analysis_table = ensure_ai_analysis_table(spark, metadata_catalog)
        target_tables = discover_target_tables(spark, metadata_catalog, target_table_name)

        results = []
        for table_name in target_tables:
            # analyze_one_table never raises — a per-table failure becomes a FAILED
            # record instead, so the loop always continues to the next table.
            record = analyze_one_table(spark, metadata_catalog, table_name)
            persist_analysis_record(spark, ai_analysis_table, record)
            results.append(record)

        elapsed_seconds = time.perf_counter() - start_time
        displayHTML(render_html_dashboard(results, elapsed_seconds))

    except Exception:
        logger.exception("AI metadata analysis run failed (infrastructure-level, not per-table).")
        raise


# COMMAND ----------

main()
