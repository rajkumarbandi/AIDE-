"""The AI Assistant's automatic agent pipeline: understand the question,
build warehouse context, generate SQL, execute it, and turn the real result
into a business-friendly answer — with SQL generation and execution
completely hidden from the user unless they open the debug view.

This is the one place in the app that both writes AND executes AI-generated
SQL with no human review step in between (by explicit design — no "Generate
SQL" button, no "Run Query" button, no editor). Two safety nets make that
acceptable, both running on every attempt, hidden from the user:
validate_select_only() (unchanged from SQL Playground — a wrong-but-still-
SELECT query can produce a wrong, retryable answer, but never a
DROP/DELETE/UPDATE/etc.) and validate_catalog_only() (added after a real
production bug: Gemini invented a plausible-looking but wrong catalog name
when its prompt context didn't spell the configured catalog out explicitly
enough — this backstop rejects any generated SQL that references a
different or missing catalog, forcing a retry, instead of letting it reach
the warehouse and fail there).
"""

import logging
from dataclasses import dataclass, field
from typing import List, Optional

import pandas as pd
import streamlit as st

from utils.config import DATA_CACHE_TTL_SECONDS, DEFAULT_CATALOG, DEFAULT_GOLD_SCHEMA
from utils.databricks import (
    DatabricksConnectionError,
    DatabricksQueryError,
    UnsafeQueryError,
    run_query,
    validate_catalog_only,
    validate_select_only,
)
from utils.gemini import (
    NO_SQL_NEEDED,
    GeminiClientError,
    GeminiConfigurationError,
    answer_general_question,
    fix_sql_from_error,
    generate_sql_from_question,
    summarize_query_result,
)
from utils.queries import GOLD_RELATIONSHIPS, SILVER_LINEAGE, sql_ai_analysis, sql_schema_columns

# The real bronze table names that actually feed the Gold sales model (every
# value across SILVER_LINEAGE) — used to filter ai_analysis business
# descriptions down to tables relevant to the Gold schema the agent queries.
# Without this, unrelated bronze tables (jobcandidate, document, department,
# ...) drown out the handful that actually matter, since 02_metadata_collector.py
# analyzes every bronze table, not just the ones behind Gold.
_RELEVANT_BRONZE_TABLES = frozenset(t for sources in SILVER_LINEAGE.values() for t in sources)

logger = logging.getLogger("aide.agent")

_MAX_SQL_ATTEMPTS = 2  # one generation + one error-corrected retry
_RESULT_PREVIEW_ROWS = 30


@dataclass
class AgentAttempt:
    """One generation-or-fix attempt, for the debug view — not shown by default."""

    sql: str
    error: Optional[str] = None


@dataclass
class AgentAnswer:
    """Everything the UI needs. `answer` is always safe to show directly;
    `sql`/`result_df`/`attempts` only render behind an opt-in debug expander.
    """

    answer: str
    sql: Optional[str] = None
    result_df: Optional[pd.DataFrame] = None
    error: Optional[str] = None
    attempts: List[AgentAttempt] = field(default_factory=list)


def _extract_business_purpose(markdown: Optional[str], max_length: int = 220) -> str:
    """Pull the real one-paragraph summary out of an ai_analysis markdown
    report (the "## Business Purpose" section) — the report's FIRST line is
    just a "# AI Metadata Analysis — <table>" title heading, not a
    description, so naively taking line one produces useless boilerplate.
    """
    if not markdown:
        return ""
    lines = str(markdown).splitlines()
    try:
        start = next(i for i, line in enumerate(lines) if line.strip().lower() == "## business purpose")
    except StopIteration:
        return ""

    purpose_lines = []
    for line in lines[start + 1 :]:
        stripped = line.strip()
        if stripped.startswith("##"):
            break
        if stripped:
            purpose_lines.append(stripped)

    purpose = " ".join(purpose_lines).strip()
    return purpose if len(purpose) <= max_length else purpose[: max_length - 1].rstrip() + "…"


@st.cache_data(ttl=DATA_CACHE_TTL_SECONDS, show_spinner=False)
def build_warehouse_context(catalog: str = DEFAULT_CATALOG, gold_schema: str = DEFAULT_GOLD_SCHEMA) -> str:
    """Real, live schema + relationship + business-description context for
    grounding SQL generation — cached so this doesn't re-query the warehouse
    on every single question asked in a session.
    """
    sections = []

    try:
        columns_df = run_query(sql_schema_columns(catalog, gold_schema))
        if not columns_df.empty:
            # Every reference is fully qualified with the real, configured catalog — omitting
            # it here previously left Gemini with no grounding for which catalog to use, and
            # it would invent a plausible-looking one (a real, observed production bug).
            lines = [
                f"{catalog}.{gold_schema}.{row['table_name']}.{row['column_name']} ({row['data_type']})"
                for _, row in columns_df.iterrows()
            ]
            sections.append("TABLES AND COLUMNS:\n" + "\n".join(lines))
    except (DatabricksConnectionError, DatabricksQueryError) as exc:
        logger.warning("Could not load schema columns for agent context: %s", exc)

    relationship_lines = [
        f"{catalog}.{gold_schema}.{rel['from_table']}.{rel['from_column']} = "
        f"{catalog}.{gold_schema}.{rel['to_table']}.{rel['to_column']} ({rel['description']})"
        for rel in GOLD_RELATIONSHIPS
    ]
    if relationship_lines:
        sections.append(
            "KNOWN RELATIONSHIPS (use these exact join keys, fully qualified):\n"
            + "\n".join(relationship_lines)
        )

    try:
        analysis_df = run_query(sql_ai_analysis(catalog=catalog))
        if not analysis_df.empty:
            desc_lines = []
            seen_tables = set()
            for _, row in analysis_df.iterrows():
                table_name = row["table_name"]
                if table_name not in _RELEVANT_BRONZE_TABLES:
                    continue
                # sql_ai_analysis orders by analysis_timestamp DESC and a table can have
                # multiple analysis runs stored — keep only the most recent per table.
                if table_name in seen_tables:
                    continue
                seen_tables.add(table_name)
                purpose = _extract_business_purpose(row.get("analysis_markdown"))
                if purpose:
                    desc_lines.append(f"{table_name}: {purpose}")
            if desc_lines:
                sections.append("BUSINESS DESCRIPTIONS:\n" + "\n".join(desc_lines))
    except (DatabricksConnectionError, DatabricksQueryError) as exc:
        logger.warning("Could not load AI analysis for agent context: %s", exc)

    return "\n\n".join(sections)


def _result_preview(result_df: pd.DataFrame) -> str:
    if result_df.empty:
        return "(no rows returned)"
    return result_df.head(_RESULT_PREVIEW_ROWS).to_string(index=False)


def answer_question(
    question: str, catalog: str = DEFAULT_CATALOG, gold_schema: str = DEFAULT_GOLD_SCHEMA
) -> AgentAnswer:
    """Run the full hidden pipeline and return a polite answer either way.

    Never raises — every failure mode (no connection, bad generation, a
    query that still fails after one retry, a Gemini outage) ends in a
    populated, business-friendly AgentAnswer.answer rather than an
    exception reaching the page.
    """
    context = build_warehouse_context(catalog, gold_schema)
    if not context:
        return AgentAnswer(
            answer="I can't reach the warehouse right now, so I can't answer questions about "
            "your data. Please check the connection in ⚙ Settings and try again.",
            error="warehouse context unavailable",
        )

    result = AgentAnswer(answer="")
    sql: Optional[str] = None
    last_error: Optional[str] = None

    for attempt_num in range(_MAX_SQL_ATTEMPTS):
        try:
            if attempt_num == 0:
                sql = generate_sql_from_question(question, context, catalog)
            else:
                sql = fix_sql_from_error(question, context, sql, last_error, catalog)
        except GeminiConfigurationError as exc:
            result.answer = "The AI service isn't configured yet — see ⚙ Settings."
            result.error = str(exc)
            return result
        except GeminiClientError as exc:
            result.answer = "I couldn't reach the AI service to work out how to answer that — please try again."
            result.error = str(exc)
            return result

        if sql.strip() == NO_SQL_NEEDED:
            try:
                result.answer = answer_general_question(question)
            except GeminiConfigurationError as exc:
                result.answer = "The AI service isn't configured yet — see ⚙ Settings."
                result.error = str(exc)
            except GeminiClientError as exc:
                result.answer = "I couldn't reach the AI service to answer that — please try again."
                result.error = str(exc)
            return result

        try:
            validate_select_only(sql)
            validate_catalog_only(sql, catalog)
            result_df = run_query(sql)
            result.sql = sql
            result.result_df = result_df
            result.attempts.append(AgentAttempt(sql=sql))
            break
        except UnsafeQueryError as exc:
            last_error = str(exc)
            result.attempts.append(AgentAttempt(sql=sql, error=last_error))
            logger.warning("Agent-generated SQL rejected: %s", exc)
        except DatabricksConnectionError as exc:
            result.answer = (
                "I can't reach the Databricks warehouse right now — please check the "
                "connection in ⚙ Settings."
            )
            result.error = str(exc)
            return result
        except DatabricksQueryError as exc:
            last_error = str(exc)
            result.attempts.append(AgentAttempt(sql=sql, error=last_error))
            logger.warning("Agent-generated SQL failed to execute (attempt %d): %s", attempt_num, exc)

    if result.result_df is None:
        result.answer = (
            "I wasn't able to answer that from the warehouse — the query I generated didn't "
            "run successfully, even after a retry. Try rephrasing the question, or ask about "
            "a specific table, territory, product, or time period."
        )
        result.error = last_error
        return result

    try:
        result.answer = summarize_query_result(
            question, result.sql, _result_preview(result.result_df), len(result.result_df)
        )
    except (GeminiConfigurationError, GeminiClientError) as exc:
        # Real results already exist even though summarization failed — say so honestly
        # instead of hiding that the underlying question WAS answered.
        result.answer = (
            f"I found the answer ({len(result.result_df)} row(s) from the warehouse) but "
            f"couldn't generate a written summary: {exc}. See the result below."
        )
        result.error = str(exc)

    return result
