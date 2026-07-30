"""Gemini client for the standalone Streamlit app.

Same architecture as notebooks/00_shared/01_gemini_client.py (singleton
client, typed exceptions, logging) — but credentials come from st.secrets
instead of dbutils.secrets, since this app runs outside Databricks.
"""

import logging

import streamlit as st
from google import genai

logger = logging.getLogger("aide.gemini")

DEFAULT_MODEL = "gemini-3.6-flash"


class GeminiClientError(Exception):
    """Raised when a Gemini API call fails or returns no usable content."""


class GeminiConfigurationError(GeminiClientError):
    """Raised when the Gemini API key is not configured."""


@st.cache_resource(show_spinner=False)
def _get_client() -> genai.Client:
    try:
        api_key = st.secrets["gemini"]["api_key"]
    except Exception as exc:
        raise GeminiConfigurationError(
            "Gemini API key is not configured. Add [gemini] api_key to "
            ".streamlit/secrets.toml."
        ) from exc
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


def generate_sql_from_question(question: str, schema_context: str) -> str:
    """A single read-only SELECT statement answering `question`.

    Grounded in `schema_context` (the real table/column list — see
    utils.queries) so the model can't invent a table or column that doesn't
    exist in this warehouse.
    """
    prompt = (
        "You are a SQL assistant for a Databricks SQL warehouse (Delta Lake, "
        "Unity Catalog, ANSI SQL). Using ONLY the tables and columns listed "
        "below — never invent a table or column name that isn't listed — "
        "write a single read-only SELECT statement that answers the question. "
        "Return ONLY the SQL statement: no markdown fences, no commentary.\n\n"
        f"Schema:\n{schema_context}\n\nQuestion: {question}"
    )
    return _strip_code_fence(generate_content(prompt))


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
