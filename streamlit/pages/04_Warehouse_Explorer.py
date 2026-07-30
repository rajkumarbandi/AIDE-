"""Warehouse Explorer — browse catalog/schema/table structure and preview data.

Foundation scope: a real table browser + bounded preview (inherently simple
once utils.databricks exists) — column-level profiling and data quality
visualizations are future scope.
"""

import streamlit as st

from components.filters import get_filters
from components.header import render_page_header
from components.tables import render_dataframe, render_empty_state
from utils.config import DEFAULT_BRONZE_SCHEMA, DEFAULT_GOLD_SCHEMA, DEFAULT_SILVER_SCHEMA
from utils.databricks import DatabricksConnectionError, DatabricksQueryError, run_query
from utils.queries import sql_information_schema_tables, sql_preview_table

render_page_header(
    title="Warehouse Explorer",
    description="Browse tables across the Bronze, Silver, and Gold schemas and preview data.",
    breadcrumb=["Home", "Warehouse Explorer"],
)

filters = get_filters()
catalog = filters["catalog"]

schema_options = [DEFAULT_BRONZE_SCHEMA, DEFAULT_SILVER_SCHEMA, DEFAULT_GOLD_SCHEMA, "metadata"]
schema = st.selectbox("Schema", schema_options, index=schema_options.index(DEFAULT_GOLD_SCHEMA))

tables_df = None
with st.spinner(f"Listing tables in {catalog}.{schema}..."):
    try:
        tables_df = run_query(sql_information_schema_tables(catalog, schema))
    except (DatabricksConnectionError, DatabricksQueryError) as exc:
        st.error(f"Could not list tables: {exc}")

if tables_df is None:
    render_empty_state("Connect to Databricks (see ⚙ Settings) to browse the warehouse.", icon="🔌")
elif tables_df.empty:
    render_empty_state(f"No tables found in `{catalog}.{schema}`.", icon="📭")
else:
    st.caption(f"{len(tables_df)} table(s) in `{catalog}.{schema}`")
    render_dataframe(tables_df)

    table_names = sorted(tables_df["table_name"].dropna().unique().tolist())
    selected_table = st.selectbox("Preview a table", table_names)
    row_limit = st.slider("Rows to preview", min_value=10, max_value=200, value=50, step=10)

    if st.button("Preview data"):
        with st.spinner(f"Loading preview of {selected_table}..."):
            try:
                preview_df = run_query(
                    sql_preview_table(catalog, schema, selected_table, limit=row_limit)
                )
                render_dataframe(preview_df)
            except (DatabricksConnectionError, DatabricksQueryError) as exc:
                st.error(f"Could not preview table: {exc}")
