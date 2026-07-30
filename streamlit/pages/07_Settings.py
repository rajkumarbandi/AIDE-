"""Settings — connection status, a real connection test, cache controls,
theme/config reference, app version, and a configuration reference.

Editing/rotating secrets from the UI is intentionally not supported (that
belongs in .streamlit/secrets.toml or a secret manager, not persisted
through this app).
"""

import streamlit as st

from components.filters import get_filters
from components.header import render_page_header
from utils.config import APP_VERSION
from utils.databricks import (
    DatabricksConnectionError,
    DatabricksQueryError,
    is_connected,
    run_query_timed,
)
from utils.gemini import GeminiClientError, GeminiConfigurationError, generate_content, is_configured

render_page_header(
    title="Settings",
    description="Connection status, cache controls, and configuration reference.",
    breadcrumb=["Home", "Settings"],
)

filters = get_filters()

st.subheader("Connection Status")
col1, col2 = st.columns(2)
with col1:
    with st.container(border=True):
        st.metric("Databricks SQL Warehouse", "Connected" if is_connected() else "Not Connected")
with col2:
    with st.container(border=True):
        st.metric("Gemini AI", "Connected" if is_configured() else "Not Connected")

st.subheader("Connection Tests")
col_db_test, col_gemini_test = st.columns(2)
with col_db_test:
    if st.button("🔌 Test Databricks Connection Now"):
        with st.spinner("Running SELECT 1 against the warehouse..."):
            try:
                _, elapsed = run_query_timed("SELECT 1")
                st.success(f"Connected — round-trip took {elapsed:.2f}s.")
            except (DatabricksConnectionError, DatabricksQueryError) as exc:
                st.error(f"Connection test failed: {exc}")
with col_gemini_test:
    if st.button("🤖 Test Gemini Connection Now"):
        with st.spinner("Sending a test prompt to Gemini..."):
            try:
                generate_content("Reply with exactly one word: OK")
                st.success("Gemini responded successfully.")
            except GeminiConfigurationError as exc:
                st.error(f"Not configured: {exc}")
            except GeminiClientError as exc:
                st.error(f"Connection test failed: {exc}")

st.subheader("Cache Controls")
st.caption(
    "Query results are cached for a few minutes to avoid re-running identical SQL on every "
    "filter change. Clear a cache below if you've just changed underlying data and want the "
    "next page load to fetch fresh results."
)
col_clear_data, col_clear_resource = st.columns(2)
with col_clear_data:
    if st.button("🔄 Refresh Metadata & Query Cache"):
        st.cache_data.clear()
        st.success("Data cache cleared — the next page view will re-run its queries.")
with col_clear_resource:
    if st.button("🔌 Reset Connection Cache"):
        st.cache_resource.clear()
        st.success("Connection cache cleared — the next query will reconnect from scratch.")

st.subheader("Current Configuration")
st.markdown(f"- **Catalog:** `{filters['catalog']}`")
st.markdown(f"- **Schema:** `{filters['schema']}`")
st.markdown(f"- **Theme:** `{filters['theme']}`")
st.markdown(f"- **App Version:** `{APP_VERSION}`")

st.subheader("Configure Secrets")
st.caption(
    "Add a `.streamlit/secrets.toml` file (never commit this — it's covered by the repo's "
    ".gitignore) with:"
)
st.code(
    """[databricks]
server_hostname = "<workspace-hostname>"
http_path = "<sql-warehouse-http-path>"
access_token = "<personal-access-token-or-service-principal-token>"

[gemini]
api_key = "<gemini-api-key>"
""",
    language="toml",
)
