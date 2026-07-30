"""Settings — connection status and configuration reference.

Foundation scope: shows how to configure secrets and displays current
connection state. Editing/rotating secrets from the UI is intentionally not
supported (that belongs in .streamlit/secrets.toml or a secret manager, not
persisted through this app).
"""

import streamlit as st

from components.filters import get_filters
from components.header import render_page_header
from utils.databricks import is_connected
from utils.gemini import is_configured

render_page_header(
    title="Settings",
    description="Connection status and configuration reference.",
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

st.subheader("Current Configuration")
st.markdown(f"- **Catalog:** `{filters['catalog']}`")
st.markdown(f"- **Schema:** `{filters['schema']}`")
st.markdown(f"- **Theme:** `{filters['theme']}`")

st.subheader("Configure Secrets")
st.caption(
    "Add a `.streamlit/secrets.toml` file (never commit this — it's already covered "
    "by the repo's .gitignore) with:"
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
