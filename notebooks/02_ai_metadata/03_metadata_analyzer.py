# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — AI Metadata Analyzer
# MAGIC
# MAGIC Production flow, now incremental and status-tracked:
# MAGIC
# MAGIC ```
# MAGIC table_metadata -> bootstrap PENDING rows -> select PENDING/FAILED -> PROCESSING
# MAGIC   -> Gemini -> Validate JSON -> Generate Markdown -> SUCCESS/FAILED -> Display HTML Summary
# MAGIC ```
# MAGIC
# MAGIC Reads table metadata from `<metadata_catalog>.metadata.table_metadata` (written by
# MAGIC `02_metadata_collector.py`, now covering Bronze, Silver, and Gold), asks Gemini for a
# MAGIC strict-JSON analysis via the shared client, validates and renders it, and **upserts**
# MAGIC (not appends) exactly one current row per table into
# MAGIC `<metadata_catalog>.metadata.ai_analysis`.
# MAGIC
# MAGIC **Incremental processing (the whole point of this rewrite):** a table already marked
# MAGIC `SUCCESS` is never re-sent to Gemini on a later run — only `PENDING` (new tables) and
# MAGIC `FAILED` (previous attempt errored) get processed. Running this notebook daily costs
# MAGIC tokens only for what's actually new or broken, not for re-analyzing everything that
# MAGIC already succeeded. See "Reprocess controls" below to force re-analysis on demand.
# MAGIC
# MAGIC **Every layer, not just Gold:** table selection is keyed by (catalog, schema, table) —
# MAGIC not table_name alone — because Bronze and Silver both have a table literally named
# MAGIC `customer` (see SILVER_LINEAGE in the Streamlit app's utils/queries.py); a bare
# MAGIC table_name key would silently conflate them. The previous version of this notebook had
# MAGIC exactly that latent bug (masked only because it had never been pointed at more than
# MAGIC one layer at once).
# MAGIC
# MAGIC **Table/layer selection** (`schema` + `target_table_name` widgets):
# MAGIC - `schema`: `bronze`, `silver`, `gold`, or `ALL` (all three)
# MAGIC - `target_table_name`: one table name, a comma-separated list, `schema.table` to
# MAGIC   disambiguate a name that exists in more than one layer, or `ALL`
# MAGIC
# MAGIC **Reprocess controls** (`force_reprocess` widget):
# MAGIC - Default (`No`): only `PENDING`/`FAILED` rows in scope are processed — "reprocess
# MAGIC   failed tables" is simply what happens automatically, every run, for free.
# MAGIC - `Yes` ("Force Reprocess"): every in-scope row (including already-`SUCCESS`) is
# MAGIC   reset to `PENDING` first, then processed — combine with `target_table_name` set to
# MAGIC   specific names for "Reprocess Selected Tables", or with `schema=<one layer>` and
# MAGIC   `target_table_name=ALL` for "Reprocess Entire Layer".
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
dbutils.widgets.text("schema", "ALL", "Schema (bronze, silver, gold, or ALL)")
dbutils.widgets.text("target_table_name", "ALL", "Target Table Name(s) (or ALL)")
dbutils.widgets.dropdown("force_reprocess", "No", ["No", "Yes"], "Force Reprocess")

# COMMAND ----------

# MAGIC %md ## Imports & logging

# COMMAND ----------

import json
import logging
import re
import time
from typing import Optional

from pyspark.sql import SparkSession
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


class AIResponseValidationError(Exception):
    """Raised when Gemini's response cannot be parsed as the expected JSON schema."""


# COMMAND ----------

# MAGIC %md ## Constants

# COMMAND ----------

PENDING, PROCESSING, SUCCESS, FAILED, SKIPPED = "PENDING", "PROCESSING", "SUCCESS", "FAILED", "SKIPPED"
ACTIVE_STATUSES = (PENDING, FAILED)  # what gets processed on a normal (non-force) run

# COMMAND ----------

# MAGIC %md ## `ai_analysis` schema, table creation, and in-place migration
# MAGIC
# MAGIC One row per (catalog, schema, table) — upserted, not appended. A table that already
# MAGIC exists from before this rewrite (append-history, no status columns) is migrated in
# MAGIC place: existing columns are checked via `information_schema.columns` first, then
# MAGIC only the genuinely-missing ones are added via `ALTER TABLE ... ADD COLUMNS` (this
# MAGIC warehouse's `ADD COLUMNS` does not support an `IF NOT EXISTS` clause — verified
# MAGIC directly, not assumed — so the check has to happen in Python instead).

# COMMAND ----------

AI_ANALYSIS_SCHEMA = StructType(
    [
        StructField("catalog_name", StringType(), nullable=False),
        StructField("schema_name", StringType(), nullable=False),
        StructField("table_name", StringType(), nullable=False),
        StructField("processing_status", StringType(), nullable=False),
        StructField("created_time", TimestampType(), nullable=False),
        StructField("last_processed_time", TimestampType(), nullable=True),
        StructField("processed_by", StringType(), nullable=True),
        StructField("ai_model_used", StringType(), nullable=True),
        StructField("token_usage", LongType(), nullable=True),
        StructField("retry_count", IntegerType(), nullable=False),
        StructField("last_error_message", StringType(), nullable=True),
        StructField("analysis_json", StringType(), nullable=True),
        StructField("analysis_markdown", StringType(), nullable=True),
        StructField("health_score", IntegerType(), nullable=True),
        StructField("confidence_score", IntegerType(), nullable=True),
        StructField("processing_time_ms", LongType(), nullable=True),
        # Kept in lockstep with last_processed_time/created_time purely so the
        # existing Streamlit app's `ORDER BY analysis_timestamp DESC` (which
        # predates this incremental rewrite) keeps working unmodified — this
        # migration is additive, not a breaking rename, on either side.
        StructField("analysis_timestamp", TimestampType(), nullable=True),
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
    """Create `<metadata_catalog>.metadata.ai_analysis` if it doesn't exist yet, using
    the current (status-tracked) schema — and if it already exists from before this
    rewrite, add whatever tracking columns are missing in place.

    `ALTER TABLE ... ADD COLUMNS IF NOT EXISTS` is NOT valid syntax on this warehouse
    — verified directly (a real PARSE_SYNTAX_ERROR), not assumed. Re-adding a column
    that already exists raises FIELD_ALREADY_EXISTS instead of being a no-op, so this
    checks information_schema.columns first and only alters what's genuinely missing.
    """
    full_table_name = f"{metadata_catalog}.metadata.ai_analysis"
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {metadata_catalog}.metadata")
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {full_table_name} ({_schema_to_ddl(AI_ANALYSIS_SCHEMA)}) "
        f"USING DELTA"
    )
    existing_columns = {
        row.column_name
        for row in spark.sql(
            f"SELECT column_name FROM {metadata_catalog}.information_schema.columns "
            f"WHERE table_schema = 'metadata' AND table_name = 'ai_analysis'"
        ).collect()
    }
    for field in AI_ANALYSIS_SCHEMA.fields:
        if field.name in existing_columns:
            continue
        sql_type = _SPARK_TYPE_TO_SQL[type(field.dataType).__name__]
        spark.sql(f"ALTER TABLE {full_table_name} ADD COLUMNS ({field.name} {sql_type})")
    return full_table_name


# COMMAND ----------

# MAGIC %md ## Table selection & incremental status bootstrap

# COMMAND ----------


def resolve_schema_filter(schema_widget_value: str) -> Optional[list]:
    """None means "no filter" (every layer); otherwise a list of schema names."""
    if schema_widget_value.strip().upper() == "ALL":
        return None
    return [schema_widget_value.strip()]


def resolve_table_filter(target_table_name_widget_value: str) -> Optional[list]:
    """None means "no filter" (every table in the schema scope); otherwise a list of
    either bare table names or "schema.table" pairs (kept as raw strings — matched
    against both forms by the caller, see build_scope_condition).
    """
    if target_table_name_widget_value.strip().upper() == "ALL":
        return None
    return [t.strip() for t in target_table_name_widget_value.split(",") if t.strip()]


def build_scope_condition(schemas: Optional[list], tables: Optional[list]) -> str:
    """A SQL WHERE fragment (no leading WHERE/AND) expressing the widget-selected
    scope — reused identically to select the work list and to know what "Force
    Reprocess"/"Reprocess Entire Layer" should reset to PENDING.
    """
    conditions = []
    if schemas:
        quoted = ", ".join(f"'{s}'" for s in schemas)
        conditions.append(f"schema_name IN ({quoted})")
    if tables:
        table_only = [t for t in tables if "." not in t]
        qualified = [t for t in tables if "." in t]
        sub_conditions = []
        if table_only:
            quoted = ", ".join(f"'{t}'" for t in table_only)
            sub_conditions.append(f"table_name IN ({quoted})")
        for qname in qualified:
            schema_part, table_part = qname.split(".", 1)
            sub_conditions.append(
                f"(schema_name = '{schema_part}' AND table_name = '{table_part}')"
            )
        if sub_conditions:
            conditions.append("(" + " OR ".join(sub_conditions) + ")")
    return " AND ".join(conditions) if conditions else "1=1"


def bootstrap_pending_rows(
    spark: SparkSession, metadata_catalog: str, ai_analysis_table: str
) -> int:
    """Insert a PENDING row for every (catalog, schema, table) present in
    table_metadata that doesn't already have a row in ai_analysis — the "New Table ->
    PENDING" step of the workflow. Returns how many new rows were added.

    Built as a pure `INSERT INTO ... SELECT` with `CAST(NULL AS ...)` literals
    rather than `spark.createDataFrame(rows, schema=...)` with Python `None` values —
    verified directly against a real run: the Spark Connect client's local-to-Arrow
    conversion path raises `PySparkValueError: input for TimestampType() must not be
    None` for a Python `None` passed as a TimestampType field, even for a nullable
    column that's about to be overwritten via `.withColumn(current_timestamp())`
    immediately after. A plain SQL NULL literal never goes through that conversion
    path at all, so it isn't subject to this restriction.
    """
    table_metadata_name = f"{metadata_catalog}.metadata.table_metadata"
    new_table_count = spark.sql(
        f"""
        SELECT COUNT(*) AS n
        FROM {table_metadata_name} m
        LEFT ANTI JOIN {ai_analysis_table} a
          ON m.catalog_name = a.catalog_name
         AND m.schema_name = a.schema_name
         AND m.table_name = a.table_name
        """
    ).collect()[0]["n"]

    if not new_table_count:
        return 0

    spark.sql(
        f"""
        INSERT INTO {ai_analysis_table}
        SELECT
            m.catalog_name,
            m.schema_name,
            m.table_name,
            '{PENDING}' AS processing_status,
            current_timestamp() AS created_time,
            CAST(NULL AS TIMESTAMP) AS last_processed_time,
            CAST(NULL AS STRING) AS processed_by,
            CAST(NULL AS STRING) AS ai_model_used,
            CAST(NULL AS BIGINT) AS token_usage,
            0 AS retry_count,
            CAST(NULL AS STRING) AS last_error_message,
            CAST(NULL AS STRING) AS analysis_json,
            CAST(NULL AS STRING) AS analysis_markdown,
            CAST(NULL AS INT) AS health_score,
            CAST(NULL AS INT) AS confidence_score,
            CAST(NULL AS BIGINT) AS processing_time_ms,
            current_timestamp() AS analysis_timestamp
        FROM {table_metadata_name} m
        LEFT ANTI JOIN {ai_analysis_table} a
          ON m.catalog_name = a.catalog_name
         AND m.schema_name = a.schema_name
         AND m.table_name = a.table_name
        """
    )
    return new_table_count


def apply_reprocess_reset(
    spark: SparkSession, ai_analysis_table: str, scope_condition: str, force_reprocess: bool
) -> int:
    """Reset in-scope rows back to PENDING before selecting the work list.

    Without --force, this resets nothing (FAILED rows are already always eligible —
    see ACTIVE_STATUSES — so "reprocess failed tables" needs no separate reset step).
    With --force, every in-scope row is reset regardless of current status, which is
    "Force Reprocess" on its own, or "Reprocess Selected Tables"/"Reprocess Entire
    Layer" depending on how narrow or broad the scope_condition is.
    """
    if not force_reprocess:
        return 0
    result = spark.sql(
        f"UPDATE {ai_analysis_table} SET processing_status = '{PENDING}' "
        f"WHERE {scope_condition}"
    )
    # UPDATE's returned DataFrame (Delta) has a num_affected_rows column in recent
    # DBR versions; fall back to 0 (informational only) if that shape isn't present.
    try:
        return result.collect()[0]["num_affected_rows"]
    except Exception:
        return 0


def get_work_list(spark: SparkSession, ai_analysis_table: str, scope_condition: str) -> list:
    """The tables actually eligible for processing THIS run: in scope AND
    PENDING/FAILED — never SUCCESS, which is the entire incremental-processing point.
    """
    statuses = ", ".join(f"'{s}'" for s in ACTIVE_STATUSES)
    rows = spark.sql(
        f"""
        SELECT catalog_name, schema_name, table_name, retry_count
        FROM {ai_analysis_table}
        WHERE {scope_condition} AND processing_status IN ({statuses})
        ORDER BY schema_name, table_name
        """
    ).collect()
    return [r.asDict() for r in rows]


# COMMAND ----------

# MAGIC %md ## Metadata retrieval, prompting, and AI response validation

# COMMAND ----------


def fetch_table_metadata(
    spark: SparkSession, metadata_catalog: str, schema_name: str, table_name: str
) -> dict:
    """Read the single metadata row for (schema_name, table_name) from the metadata
    repository — keyed by the full pair, not table_name alone (see module docstring:
    Bronze and Silver can both have a table literally named the same thing).
    """
    full_table_name = f"{metadata_catalog}.metadata.table_metadata"
    matches = (
        spark.table(full_table_name)
        .filter((F.col("schema_name") == schema_name) & (F.col("table_name") == table_name))
        .collect()
    )
    if not matches:
        raise ValueError(
            f"No metadata found for '{schema_name}.{table_name}' in '{full_table_name}'. "
            f"Has 02_metadata_collector.py been run for this layer?"
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

# MAGIC %md ## Per-table orchestration — the PENDING/PROCESSING/SUCCESS/FAILED workflow

# COMMAND ----------


def get_processed_by(spark: SparkSession) -> str:
    """The real identity this notebook is running as — `current_user()` reflects
    whoever/whatever the job or interactive session authenticates as, not a
    fabricated value.
    """
    return spark.sql("SELECT current_user() AS user").collect()[0]["user"]


def mark_processing(spark: SparkSession, ai_analysis_table: str, key: dict) -> None:
    spark.sql(
        f"""
        UPDATE {ai_analysis_table}
        SET processing_status = '{PROCESSING}', last_processed_time = current_timestamp()
        WHERE catalog_name = '{key["catalog_name"]}' AND schema_name = '{key["schema_name"]}'
          AND table_name = '{key["table_name"]}'
        """
    )


def analyze_one_table(spark: SparkSession, metadata_catalog: str, key: dict) -> dict:
    """Run metadata fetch -> prompt -> Gemini -> validate -> markdown for one table.

    Always returns a result dict describing the outcome — never raises; any failure
    along the way is captured as a FAILED result instead, so the caller's loop always
    continues to the next table.
    """
    start = time.perf_counter()
    schema_name, table_name = key["schema_name"], key["table_name"]

    try:
        metadata = fetch_table_metadata(spark, metadata_catalog, schema_name, table_name)
        prompt = build_prompt(metadata)
        raw_response = generate_content(prompt)
        analysis = parse_ai_json_response(raw_response)
        markdown = render_markdown(table_name, analysis)
        processing_time_ms = int((time.perf_counter() - start) * 1000)
        logger.info(
            "Analyzed '%s.%s' successfully in %d ms.", schema_name, table_name, processing_time_ms
        )
        return {
            "status": SUCCESS,
            "analysis_json": json.dumps(analysis),
            "analysis_markdown": markdown,
            "health_score": _coerce_score(analysis.get("health_score")),
            "confidence_score": _coerce_score(analysis.get("confidence_score")),
            "processing_time_ms": processing_time_ms,
            "error_message": None,
        }
    except Exception as exc:
        processing_time_ms = int((time.perf_counter() - start) * 1000)
        logger.error("Analysis failed for '%s.%s': %s", schema_name, table_name, exc)
        return {
            "status": FAILED,
            "analysis_json": None,
            "analysis_markdown": None,
            "health_score": None,
            "confidence_score": None,
            "processing_time_ms": processing_time_ms,
            "error_message": str(exc),
        }


def persist_result(
    spark: SparkSession, ai_analysis_table: str, key: dict, result: dict, processed_by: str
) -> None:
    """Update the one row for this table in place — an UPDATE, not an append/MERGE,
    since bootstrap_pending_rows() already guarantees the row exists before this is
    ever called (either from the original bootstrap or a prior run).
    """
    retry_count_expr = (
        "retry_count" if result["status"] == SUCCESS else "retry_count + 1"
    )
    set_clauses = [
        f"processing_status = '{result['status']}'",
        "last_processed_time = current_timestamp()",
        "analysis_timestamp = current_timestamp()",
        f"processed_by = '{processed_by}'",
        f"ai_model_used = '{DEFAULT_MODEL}'",
        f"retry_count = {retry_count_expr}",
        f"analysis_json = {_sql_literal(result['analysis_json'])}",
        f"analysis_markdown = {_sql_literal(result['analysis_markdown'])}",
        f"health_score = {_sql_literal(result['health_score'])}",
        f"confidence_score = {_sql_literal(result['confidence_score'])}",
        f"processing_time_ms = {_sql_literal(result['processing_time_ms'])}",
        f"last_error_message = {_sql_literal(result['error_message'])}",
    ]
    spark.sql(
        f"""
        UPDATE {ai_analysis_table}
        SET {", ".join(set_clauses)}
        WHERE catalog_name = '{key["catalog_name"]}' AND schema_name = '{key["schema_name"]}'
          AND table_name = '{key["table_name"]}'
        """
    )


def _sql_literal(value) -> str:
    """A safe SQL literal for a value already produced by this pipeline (never raw
    user text) — NULL, a quoted/escaped string, or a bare number.
    """
    if value is None:
        return "NULL"
    if isinstance(value, (int, float)):
        return str(value)
    return "'" + str(value).replace("'", "''") + "'"


# COMMAND ----------

# MAGIC %md ## HTML dashboard rendering
# MAGIC
# MAGIC Pure presentation, isolated from the processing/persistence logic above.

# COMMAND ----------

_HEALTH_BUCKETS = [
    ("Healthy (80-100)", 80, 100, "#16a34a"),
    ("Needs Review (50-79)", 50, 79, "#d97706"),
    ("Poor (0-49)", 0, 49, "#dc2626"),
]


def _status_badge(status: str) -> str:
    colors = {SUCCESS: "#16a34a", FAILED: "#dc2626", SKIPPED: "#6b7280"}
    color = colors.get(status, "#6b7280")
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


def render_html_dashboard(
    results: list, skipped_count: int, new_pending_count: int, total_elapsed_seconds: float
) -> str:
    """Build the enterprise-style HTML summary dashboard for displayHTML()."""
    total = len(results)
    succeeded = sum(1 for r in results if r["status"] == SUCCESS)
    failed = total - succeeded

    table_rows = "".join(
        f"<tr>"
        f"<td>{r['schema_name']}.{r['table_name']}</td>"
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
    Incremental run: only PENDING/FAILED tables were sent to Gemini this time.
  </p>

  <div style="display:flex;gap:16px;margin:20px 0;flex-wrap:wrap;">
    <div style="background:#f3f4f6;border-radius:10px;padding:16px 24px;min-width:140px;">
      <div style="font-size:28px;font-weight:700;">{total}</div>
      <div style="color:#6b7280;font-size:13px;">Processed This Run</div>
    </div>
    <div style="background:#ecfdf5;border-radius:10px;padding:16px 24px;min-width:140px;">
      <div style="font-size:28px;font-weight:700;color:#16a34a;">{succeeded}</div>
      <div style="color:#6b7280;font-size:13px;">Successful</div>
    </div>
    <div style="background:#fef2f2;border-radius:10px;padding:16px 24px;min-width:140px;">
      <div style="font-size:28px;font-weight:700;color:#dc2626;">{failed}</div>
      <div style="color:#6b7280;font-size:13px;">Failed</div>
    </div>
    <div style="background:#f3f4f6;border-radius:10px;padding:16px 24px;min-width:140px;">
      <div style="font-size:28px;font-weight:700;color:#6b7280;">{skipped_count}</div>
      <div style="color:#6b7280;font-size:13px;">Skipped (already SUCCESS)</div>
    </div>
    <div style="background:#eff6ff;border-radius:10px;padding:16px 24px;min-width:140px;">
      <div style="font-size:28px;font-weight:700;color:#2563eb;">{total_elapsed_seconds:.1f}s</div>
      <div style="color:#6b7280;font-size:13px;">Execution Time</div>
    </div>
  </div>

  <p style="color:#6b7280;font-size:13px;">
    {new_pending_count} newly-discovered table(s) added as PENDING this run
    (processed immediately if in scope, otherwise ready for the next run).
  </p>

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
    schema_widget_value = dbutils.widgets.get("schema")
    target_table_name = dbutils.widgets.get("target_table_name")
    force_reprocess = dbutils.widgets.get("force_reprocess").strip().lower() == "yes"

    logger.info(
        "Starting AI metadata analysis | catalog=%s schema=%s target=%s force_reprocess=%s",
        metadata_catalog,
        schema_widget_value,
        target_table_name,
        force_reprocess,
    )

    start_time = time.perf_counter()

    try:
        ai_analysis_table = ensure_ai_analysis_table(spark, metadata_catalog)
        new_pending_count = bootstrap_pending_rows(spark, metadata_catalog, ai_analysis_table)

        schemas = resolve_schema_filter(schema_widget_value)
        tables = resolve_table_filter(target_table_name)
        scope_condition = build_scope_condition(schemas, tables)

        reset_count = apply_reprocess_reset(
            spark, ai_analysis_table, scope_condition, force_reprocess
        )
        if force_reprocess:
            logger.info("Force reprocess: reset %d row(s) to PENDING.", reset_count)

        work_list = get_work_list(spark, ai_analysis_table, scope_condition)
        in_scope_total = spark.sql(
            f"SELECT COUNT(*) AS n FROM {ai_analysis_table} WHERE {scope_condition}"
        ).collect()[0]["n"]
        skipped_count = in_scope_total - len(work_list)

        processed_by = get_processed_by(spark)
        results = []
        for key in work_list:
            mark_processing(spark, ai_analysis_table, key)
            outcome = analyze_one_table(spark, metadata_catalog, key)
            persist_result(spark, ai_analysis_table, key, outcome, processed_by)
            results.append({**key, **outcome})

        elapsed_seconds = time.perf_counter() - start_time
        displayHTML(
            render_html_dashboard(results, skipped_count, new_pending_count, elapsed_seconds)
        )

    except Exception:
        logger.exception("AI metadata analysis run failed (infrastructure-level, not per-table).")
        raise


# COMMAND ----------

main()
