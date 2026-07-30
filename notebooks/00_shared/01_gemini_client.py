# Databricks notebook source
# MAGIC %md
# MAGIC # Gemini Client — Reusable Utility
# MAGIC
# MAGIC A single, reusable `generate_content(prompt, model)` function for calling the Gemini
# MAGIC API, backed by a lazily-initialized, singleton client. Import this notebook from any
# MAGIC other notebook with:
# MAGIC
# MAGIC ```
# MAGIC %run "../00_shared/01_gemini_client"
# MAGIC ```
# MAGIC
# MAGIC after which `generate_content(...)` is available directly.
# MAGIC
# MAGIC **Dependency:** requires the `google-genai` package on the cluster (cluster-scoped
# MAGIC library, pinned to a specific version per environment). Deliberately **not**
# MAGIC installed via `%pip install` in this notebook — `%pip install` restarts the Python
# MAGIC interpreter, which would wipe out the state of whatever notebook `%run`s this one.
# MAGIC
# MAGIC **Secrets:** the API key is never hardcoded. It is read once from Databricks Secrets
# MAGIC (scope `kv-devsandbox`, key `gemini-AIapikey`, itself backed by Azure Key Vault) the
# MAGIC first time a client is needed, and reused for the lifetime of the notebook session.

# COMMAND ----------

# MAGIC %md ## Imports & logging

# COMMAND ----------

import logging
from typing import Optional

from google import genai

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
logger = logging.getLogger("aide.gemini_client")

# COMMAND ----------

# MAGIC %md ## Exceptions

# COMMAND ----------


class GeminiClientError(Exception):
    """Raised when a Gemini API call fails or returns no usable content."""


class GeminiConfigurationError(GeminiClientError):
    """Raised when the Gemini API key cannot be retrieved from Databricks Secrets."""


# COMMAND ----------

# MAGIC %md ## Constants

# COMMAND ----------

SECRET_SCOPE = "kv-devsandbox"
SECRET_KEY = "gemini-AIapikey"
DEFAULT_MODEL = "gemini-3.6-flash"

# COMMAND ----------

# MAGIC %md ## Client initialization (singleton)

# COMMAND ----------

_client: Optional[genai.Client] = None  # module-level singleton; set on first use


def _get_api_key() -> str:
    """Read the Gemini API key from Databricks Secrets. Never hardcode this value."""
    try:
        return dbutils.secrets.get(scope=SECRET_SCOPE, key=SECRET_KEY)
    except Exception as exc:
        logger.error(
            "Failed to read Gemini API key from secret scope '%s', key '%s': %s",
            SECRET_SCOPE,
            SECRET_KEY,
            exc,
        )
        raise GeminiConfigurationError(
            f"Could not read the Gemini API key from secret scope '{SECRET_SCOPE}' "
            f"(key '{SECRET_KEY}'). Verify the scope/key exist and this cluster's "
            f"principal has access."
        ) from exc


def _get_client() -> genai.Client:
    """Return the shared Gemini client, creating it on first use only.

    The API key is read from secrets exactly once per session — here, at client
    construction — not on every generate_content() call.
    """
    global _client
    if _client is None:
        api_key = _get_api_key()
        _client = genai.Client(api_key=api_key)
        logger.info("Gemini client initialized.")
    return _client


# COMMAND ----------

# MAGIC %md ## generate_content()

# COMMAND ----------


def generate_content(prompt: str, model: str = DEFAULT_MODEL) -> str:
    """Send `prompt` to Gemini and return the response text.

    Raises:
        ValueError: if `prompt` is empty or whitespace-only.
        GeminiConfigurationError: if the API key cannot be read from secrets.
        GeminiClientError: if the API call fails, or Gemini returns no usable text
            (e.g. blocked by a safety filter).
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
        logger.error("Gemini returned no usable text (model=%s).", model)
        raise GeminiClientError(
            f"Gemini returned an empty response for model '{model}' "
            f"(possibly blocked by a safety filter)."
        )

    return text
