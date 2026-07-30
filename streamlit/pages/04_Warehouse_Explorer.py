"""Warehouse Explorer — browse catalog/schema/table structure, preview data,
and profile columns (types, null %, distinct %, sample values, distribution),
with generated SQL and CSV export throughout.

Column profiling runs one combined query for null/distinct counts across all
columns (utils.queries.sql_column_profile) rather than one query per column —
sample values and the distribution chart are the only per-column queries, and
those are opt-in (a selectbox + the profiling table already shown), not run
eagerly for every column on page load.
"""

import pandas as pd
import streamlit as st

from components.charts import render_bar_chart
from components.filters import get_filters
from components.header import render_page_header
from components.tables import render_dataframe, render_empty_state
from utils.config import DEFAULT_BRONZE_SCHEMA, DEFAULT_GOLD_SCHEMA, DEFAULT_SILVER_SCHEMA
from utils.databricks import DatabricksConnectionError, DatabricksQueryError, run_query
from utils.helpers import format_pct
from utils.queries import (
    sql_column_distribution,
    sql_column_profile,
    sql_column_sample_values,
    sql_information_schema_columns,
    sql_information_schema_tables,
    sql_preview_table,
)

render_page_header(
    title="Warehouse Explorer",
    description="Browse tables across the Bronze, Silver, and Gold schemas, preview data, "
    "and profile columns.",
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
    selected_table = st.selectbox("Select a table", table_names)

    preview_tab, profile_tab = st.tabs(["📄 Data Preview", "📊 Column Profiling"])

    with preview_tab:
        row_limit = st.slider("Rows to preview", min_value=10, max_value=200, value=50, step=10)
        if st.button("Preview data"):
            with st.spinner(f"Loading preview of {selected_table}..."):
                try:
                    preview_df = run_query(
                        sql_preview_table(catalog, schema, selected_table, limit=row_limit)
                    )
                    render_dataframe(preview_df)
                    if not preview_df.empty:
                        st.download_button(
                            "⬇ Download as CSV",
                            data=preview_df.to_csv(index=False),
                            file_name=f"{selected_table}_preview.csv",
                            mime="text/csv",
                            key="download_preview",
                        )
                except (DatabricksConnectionError, DatabricksQueryError) as exc:
                    st.error(f"Could not preview table: {exc}")

        with st.expander("View generated SQL"):
            st.code(sql_preview_table(catalog, schema, selected_table, limit=row_limit), language="sql")

    with profile_tab:
        try:
            columns_df = run_query(sql_information_schema_columns(catalog, schema, selected_table))
        except (DatabricksConnectionError, DatabricksQueryError) as exc:
            st.error(f"Could not load column list: {exc}")
            columns_df = None

        if columns_df is None or columns_df.empty:
            render_empty_state("No column metadata available for this table.")
        else:
            column_names = columns_df["column_name"].tolist()
            profile_sql = sql_column_profile(catalog, schema, selected_table, column_names)

            try:
                profile_df = run_query(profile_sql)
            except (DatabricksConnectionError, DatabricksQueryError) as exc:
                st.error(f"Could not profile columns: {exc}")
                profile_df = None

            if profile_df is None or profile_df.empty:
                render_empty_state("Could not compute a column profile for this table.")
            else:
                profile_row = profile_df.iloc[0]
                total_rows = int(profile_row.get("total_rows") or 0)
                st.caption(f"{total_rows:,} total row(s)")

                summary_rows = []
                for _, col_row in columns_df.iterrows():
                    name = col_row["column_name"]
                    nulls = profile_row.get(f"{name}__nulls")
                    distinct = profile_row.get(f"{name}__distinct")
                    null_pct = (nulls / total_rows) if (total_rows and nulls is not None) else None
                    distinct_pct = (
                        (distinct / total_rows) if (total_rows and distinct is not None) else None
                    )
                    summary_rows.append(
                        {
                            "column_name": name,
                            "data_type": col_row["data_type"],
                            "is_nullable": col_row["is_nullable"],
                            "null_%": format_pct(null_pct),
                            "approx_distinct_%": format_pct(distinct_pct),
                        }
                    )

                render_dataframe(pd.DataFrame(summary_rows), empty_message="No columns to profile.")

                with st.expander("View generated profiling SQL"):
                    st.code(profile_sql, language="sql")

                st.divider()
                st.subheader("Explore a single column")
                explore_column = st.selectbox("Column", column_names, key="profile_explore_column")

                col_a, col_b = st.columns(2)
                with col_a:
                    st.markdown("**Sample Values**")
                    try:
                        samples_df = run_query(
                            sql_column_sample_values(catalog, schema, selected_table, explore_column)
                        )
                        render_dataframe(samples_df, empty_message="No non-null sample values.")
                    except (DatabricksConnectionError, DatabricksQueryError) as exc:
                        st.error(f"Could not load sample values: {exc}")

                with col_b:
                    st.markdown("**Value Distribution (Top 15)**")
                    try:
                        dist_df = run_query(
                            sql_column_distribution(catalog, schema, selected_table, explore_column)
                        )
                        if dist_df is not None and not dist_df.empty:
                            dist_df["value"] = dist_df["value"].astype(str)
                            render_bar_chart(dist_df, x="value", y="frequency")
                        else:
                            render_empty_state("No distribution data available.")
                    except (DatabricksConnectionError, DatabricksQueryError) as exc:
                        st.error(f"Could not load value distribution: {exc}")
