"""Gemini client for the standalone Streamlit app.

Same architecture as notebooks/00_shared/01_gemini_client.py (singleton
client, typed exceptions, logging) — but credentials come from st.secrets
instead of dbutils.secrets, since this app runs outside Databricks.
"""

import logging

import streamlit as st
from google import genai

from utils.secrets import get_setting

logger = logging.getLogger("aide.gemini")

DEFAULT_MODEL = "gemini-3.6-flash"


class GeminiClientError(Exception):
    """Raised when a Gemini API call fails or returns no usable content."""


class GeminiConfigurationError(GeminiClientError):
    """Raised when the Gemini API key is not configured."""


@st.cache_resource(show_spinner=False)
def _get_client() -> genai.Client:
    """Resolve the API key: environment variable (Databricks Apps' documented
    secrets mechanism) first, then st.secrets (local dev).
    """
    api_key = get_setting("GEMINI_API_KEY", ("gemini", "api_key"))
    if not api_key:
        raise GeminiConfigurationError(
            "Gemini API key is not configured. Set GEMINI_API_KEY as an environment "
            "variable (Databricks Apps — see app.yaml) or add [gemini] api_key to "
            ".streamlit/secrets.toml (local development)."
        )
    return genai.Client(api_key=api_key)


def generate_content(prompt: str, model: str = DEFAULT_MODEL) -> str:
    """Send `prompt` to Gemini and return the response text.

    Raises ValueError/GeminiConfigurationError/GeminiClientError — callers
    should catch these and render the page's error state, never let them
    crash the app.
    """
    if not prompt or not prompt.strip():
        raise ValueError("prompt must be a non-empty string.")

    client = _get_client()
    try:
        response = client.models.generate_content(model=model, contents=prompt)
    except Exception as exc:
        logger.error("Gemini generate_content call failed (model=%s): %s", model, exc)
        raise GeminiClientError(f"Gemini API call failed for model '{model}': {exc}") from exc

    text = getattr(response, "text", None)
    if not text:
        raise GeminiClientError(f"Gemini returned an empty response for model '{model}'.")
    return text


def is_configured() -> bool:
    """Best-effort configuration check for the sidebar status indicator."""
    try:
        _get_client()
        return True
    except Exception:
        return False


def _strip_code_fence(text: str) -> str:
    """Defensively strip a markdown code fence if the model added one despite
    being told not to — same defensive pattern used by the notebooks' AI
    Metadata Analyzer (notebooks/02_ai_metadata/03_metadata_analyzer_poc.py).
    """
    cleaned = text.strip()
    if cleaned.startswith("```"):
        lines = cleaned.split("\n")
        if lines and lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        cleaned = "\n".join(lines).strip()
    return cleaned


def generate_executive_summary(kpi_context: str) -> str:
    """A short narrative summary of already-computed KPI/SQL results.

    `kpi_context` must already contain the real figures (formatted as text) —
    the prompt explicitly forbids inventing numbers not present in it, so this
    narrates real query results, it doesn't fabricate them.
    """
    prompt = (
        "You are a data analyst summarizing sales performance for executives. "
        "Using ONLY the figures below — do not invent any number not present "
        "here — write a concise 3-5 sentence executive summary highlighting "
        "the most notable trends and contributors.\n\nData:\n" + kpi_context
    )
    return generate_content(prompt)


NO_SQL_NEEDED = "NO_SQL_NEEDED"


def _catalog_mandate(catalog: str) -> str:
    """Shared, unmissable instruction block: exactly one catalog is ever
    valid, and it's passed explicitly here — not left for the model to infer
    from the schema listing alone. A real production bug (Gemini inventing a
    plausible-looking but wrong catalog name, e.g. "rg_dev_sandbox") happened
    specifically because the catalog previously only appeared buried inside
    the schema text; this repeats it as its own explicit, emphatic rule.
    """
    return (
        f"The ONLY valid catalog is exactly \"{catalog}\". Every single table reference in "
        f"your SQL — every FROM and every JOIN — MUST be fully qualified as "
        f"{catalog}.<schema>.<table>. Never omit the catalog. Never invent, guess, or "
        f"substitute a different catalog name. Never use current_catalog(), a workspace "
        f"default catalog, or a user default catalog — only the literal string \"{catalog}\"."
    )


def generate_sql_from_question(question: str, schema_context: str, catalog: str) -> str:
    """A single read-only SELECT statement answering `question` — or the
    literal string NO_SQL_NEEDED if the question isn't about this
    warehouse's data (e.g. a general/conceptual question), so the AI
    Assistant's agent loop can route it to answer_general_question() instead
    of wasting an execution attempt on a query that was never the point.

    Grounded in `schema_context` (the real table/column list, relationships,
    and business descriptions — see utils.agent) so the model can't invent a
    table or column that doesn't exist in this warehouse. `catalog` is the
    single configured catalog every reference must use — see _catalog_mandate.
    """
    prompt = (
        "You are a SQL assistant for a Databricks SQL warehouse (Delta Lake, "
        "Unity Catalog, ANSI SQL). If the question below is a general/conceptual "
        "question NOT asking about this warehouse's actual data (e.g. asking what a "
        "medallion architecture is, or how this app works), respond with EXACTLY "
        f"the text {NO_SQL_NEEDED} and nothing else.\n\n"
        "Otherwise, using ONLY the tables and columns listed below — never invent a "
        "table or column name that isn't listed — write a single read-only SELECT "
        "statement that answers the question, using the listed relationships for "
        "join keys. Return ONLY the SQL statement: no markdown fences, no commentary.\n\n"
        f"{_catalog_mandate(catalog)}\n\n"
        f"Schema:\n{schema_context}\n\nQuestion: {question}"
    )
    return _strip_code_fence(generate_content(prompt))


def fix_sql_from_error(
    question: str, schema_context: str, failed_sql: str, error_message: str, catalog: str
) -> str:
    """A corrected SELECT statement after `failed_sql` errored against the
    warehouse — the AI Assistant's one automatic retry, grounded in the same
    real schema context plus the actual error text, so the retry has a
    concrete reason to differ rather than repeating the same mistake.
    `catalog` is repeated here too: a wrong-catalog failure must not be
    "fixed" into a different wrong catalog.
    """
    prompt = (
        "The following SQL query failed against a Databricks SQL warehouse. Using "
        "ONLY the tables and columns listed below, fix the query so it correctly "
        "answers the original question. Return ONLY the corrected SQL statement: no "
        "markdown fences, no commentary.\n\n"
        f"{_catalog_mandate(catalog)}\n\n"
        f"Schema:\n{schema_context}\n\nOriginal question: {question}\n\n"
        f"Failed SQL:\n{failed_sql}\n\nError:\n{error_message}"
    )
    return _strip_code_fence(generate_content(prompt))


def answer_general_question(question: str) -> str:
    """A direct, general-knowledge answer for a question that isn't about
    this warehouse's actual data (routed here via generate_sql_from_question
    returning NO_SQL_NEEDED) — e.g. "what is a medallion architecture?".
    """
    prompt = (
        "You are AIDE's AI Data Analyst, embedded in an enterprise data engineering "
        "platform built on Databricks, Delta Lake, and a medallion (Bronze/Silver/"
        "Gold) architecture over the AdventureWorks dataset. The user asked a general "
        "question, not one about this warehouse's live data. Answer concisely and "
        "helpfully, ChatGPT-style — no long essays.\n\nQuestion: " + question
    )
    return generate_content(prompt)


def summarize_query_result(question: str, sql: str, result_preview: str, row_count: int) -> str:
    """Turn real, already-executed query results into a concise, business-
    friendly answer — never asked to invent figures, only to narrate the
    real rows given to it.
    """
    prompt = (
        "You are AIDE's AI Data Analyst. A user asked a business question about a "
        "Databricks sales data warehouse; you already ran a SQL query and have REAL "
        "results below — use ONLY these figures, never invent a number not present "
        "here. Answer like a professional analyst message (ChatGPT-style): concise, "
        "business-friendly, and never mention SQL or that a query was run. Use this "
        "structure, omitting any section that doesn't apply, and keep it tight (no "
        "long essays):\n\n"
        "**Summary** — one or two sentences directly answering the question.\n"
        "**Key Findings** — the most important numbers; use a short markdown table if "
        "there are multiple rows.\n"
        "**Insights** — a sentence of context, only if it adds real value.\n"
        "**Recommendations** — only if genuinely relevant to a business question like this.\n\n"
        f"Question: {question}\n\nSQL that was run (context only — never mention or repeat "
        f"this in your answer):\n{sql}\n\nResult ({row_count} row(s) total, showing up to 30):\n"
        f"{result_preview}"
    )
    return generate_content(prompt)


def explain_sql(sql: str) -> str:
    """Plain-language explanation of what a SQL query does."""
    prompt = (
        "Explain what the following SQL query does, in plain language, for a "
        "business user with no SQL background. Be concise.\n\nSQL:\n" + sql
    )
    return generate_content(prompt)


def optimize_sql(sql: str) -> str:
    """Plain-language, concrete optimization suggestions for a SQL query."""
    prompt = (
        "Review the following SQL query for a Databricks SQL warehouse (Delta "
        "Lake) and suggest concrete performance optimizations (e.g. filter "
        "pushdown, avoiding SELECT *, join order, partition/Z-order awareness). "
        "Be specific and concise. If the query already looks efficient, say so "
        "rather than inventing an issue.\n\nSQL:\n" + sql
    )
    return generate_content(prompt)


def explain_relationship(description: str) -> str:
    """Plain-language elaboration on an already-known warehouse relationship.

    `description` should be the real, already-documented relationship text
    (from utils.queries.GOLD_RELATIONSHIPS) — Gemini elaborates on a real
    relationship, it doesn't invent one.
    """
    prompt = (
        "You are a data architect. In 2-3 sentences, explain the business "
        "meaning and typical analytical use of the following warehouse table "
        f"relationship, for someone new to this data model:\n\n{description}"
    )
    return generate_content(prompt)
