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
