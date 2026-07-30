"""AI Data Catalog — browse every table's collected metadata and AI-generated
analysis (from notebooks/02_ai_metadata/02_metadata_collector.py and
03_metadata_analyzer_poc.py).

Foundation scope: a real, working catalog browser (list + search + detail),
since that's inherently simple once utils.databricks exists — deeper features
(search ranking, tagging, approval workflow) are future scope.
"""

import streamlit as st

from components.filters import get_filters
from components.header import render_page_header
from components.tables import render_dataframe, render_empty_state
from utils.databricks import DatabricksConnectionError, DatabricksQueryError, run_query
from utils.queries import sql_ai_analysis, sql_table_metadata

render_page_header(
    title="AI Data Catalog",
    description="Every table's collected metadata and AI-generated business analysis.",
    breadcrumb=["Home", "AI Data Catalog"],
)

filters = get_filters()

metadata_df = None
with st.spinner("Loading table catalog..."):
    try:
        metadata_df = run_query(sql_table_metadata(catalog=filters["catalog"]))
    except (DatabricksConnectionError, DatabricksQueryError) as exc:
        st.error(f"Could not load the data catalog: {exc}")

if metadata_df is None:
    render_empty_state(
        "Connect to Databricks (see ⚙ Settings) to browse "
        f"`{filters['catalog']}.metadata.table_metadata`.",
        icon="🔌",
    )
elif metadata_df.empty:
    render_empty_state(
        "table_metadata has no rows yet — run 02_metadata_collector.py first.", icon="📭"
    )
else:
    search = st.text_input("Search tables", placeholder="e.g. customer")
    filtered_df = metadata_df
    if search:
        filtered_df = metadata_df[
            metadata_df["table_name"].str.contains(search, case=False, na=False)
        ]

    st.caption(f"{len(filtered_df)} of {len(metadata_df)} tables")
    render_dataframe(
        filtered_df[
            ["catalog_name", "schema_name", "table_name", "column_count", "row_count"]
        ]
        if not filtered_df.empty
        else filtered_df
    )

    table_names = sorted(filtered_df["table_name"].dropna().unique().tolist())
    if table_names:
        selected_table = st.selectbox("View AI analysis for", table_names)
        with st.spinner("Loading AI analysis..."):
            try:
                analysis_df = run_query(
                    sql_ai_analysis(catalog=filters["catalog"], table_name=selected_table)
                )
            except (DatabricksConnectionError, DatabricksQueryError) as exc:
                st.error(f"Could not load AI analysis: {exc}")
                analysis_df = None

        if analysis_df is None or analysis_df.empty:
            render_empty_state(
                f"No AI analysis found for '{selected_table}' yet — run "
                "03_metadata_analyzer_poc.py for this table.",
                icon="🤖",
            )
        else:
            latest = analysis_df.iloc[0]
            col1, col2, col3 = st.columns(3)
            col1.metric("Health Score", latest.get("health_score", "—"))
            col2.metric("Confidence Score", latest.get("confidence_score", "—"))
            col3.metric("Status", latest.get("status", "—"))
            with st.container(border=True):
                st.markdown(latest.get("analysis_markdown") or "_No markdown content stored._")
