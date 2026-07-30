"""SQL Playground — run ad-hoc SQL against the warehouse and see results.

Foundation scope: real execution (text box -> run -> render) — query history,
saved queries, and AI-assisted SQL generation are future scope.
"""

import streamlit as st

from components.filters import get_filters
from components.header import render_page_header
from components.tables import render_dataframe
from utils.databricks import DatabricksConnectionError, DatabricksQueryError, run_query

render_page_header(
    title="SQL Playground",
    description="Run read-only SQL directly against the warehouse.",
    breadcrumb=["Home", "SQL Playground"],
)

filters = get_filters()

default_query = (
    f"SELECT * FROM {filters['catalog']}.{filters['schema']}.sales_kpi_summary LIMIT 10"
)
query = st.text_area("SQL query", value=default_query, height=140)

run_clicked = st.button("▶ Run Query", type="primary")

if run_clicked:
    if not query.strip():
        st.warning("Enter a query first.")
    else:
        with st.spinner("Running query..."):
            try:
                result_df = run_query(query)
                st.success(f"{len(result_df)} row(s) returned.")
                render_dataframe(result_df)
            except DatabricksConnectionError as exc:
                st.error(f"Not connected to Databricks: {exc}")
            except DatabricksQueryError as exc:
                st.error(f"Query failed: {exc}")

st.caption("⚠ This runs directly against the connected SQL Warehouse — use caution "
           "with anything beyond SELECT statements.")
