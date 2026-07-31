"""AI Data Catalog — search, filter, and sort every collected table, with a
full detail view (columns, AI explanation, sample SQL, related tables, data
quality metrics) on selection.

Honesty note: "Tags" and "Owner" are not fields that exist anywhere in
metadata.table_metadata or metadata.ai_analysis — there is no real data
source for them yet, so they're shown as genuinely empty ("Not set"), never
fabricated. "Source System" is shown as "AdventureWorks" because that's a
true fact about this specific warehouse, not an invented value.
"""

import pandas as pd
import streamlit as st

from components.filters import get_filters
from components.header import render_page_header
from components.metric_cards import render_metric_grid
from components.shell import render_app_shell
from components.tables import render_dataframe, render_empty_state
from utils.databricks import DatabricksConnectionError, DatabricksQueryError, run_query
from utils.gemini import GeminiClientError, GeminiConfigurationError, generate_content
from utils.helpers import format_number, format_pct, parse_nested_field, to_plain_list
from utils.queries import (
    GOLD_LINEAGE,
    GOLD_RELATIONSHIPS,
    SILVER_LINEAGE,
    sql_ai_analysis,
    sql_information_schema_columns,
    sql_table_metadata,
)

render_app_shell()

render_page_header(
    title="AI Data Catalog",
    description="Search, filter, and explore every table's collected metadata and "
    "AI-generated business analysis.",
    breadcrumb=["Home", "AI Data Catalog"],
)

filters = get_filters()
catalog = filters["catalog"]


def _related_tables(table_name: str) -> list:
    """Real relationships only — from the same registries the AI Data Model
    Explorer uses, not invented per-table.
    """
    related = set()
    for rel in GOLD_RELATIONSHIPS:
        if rel["from_table"] == table_name:
            related.add(rel["to_table"])
        elif rel["to_table"] == table_name:
            related.add(rel["from_table"])
    for silver_table, bronze_sources in SILVER_LINEAGE.items():
        if table_name == silver_table:
            related.update(bronze_sources)
        elif table_name in bronze_sources:
            related.add(silver_table)
    for gold_table, silver_sources in GOLD_LINEAGE.items():
        if table_name == gold_table:
            related.update(silver_sources)
        elif table_name in silver_sources:
            related.add(gold_table)
    return sorted(related)


def _render_table_detail(table_name: str, meta_row: pd.Series) -> None:
    st.subheader(f"📄 {table_name.replace('_', ' ').title()}")
    st.caption(f"Business Name: {table_name.replace('_', ' ').title()} · Source System: AdventureWorks")

    render_metric_grid(
        [
            {"label": "Layer", "value": str(meta_row.get("schema_name", "—")).title()},
            {"label": "Row Count", "value": format_number(meta_row.get("row_count"))},
            {"label": "Columns", "value": str(meta_row.get("column_count", "—"))},
            {"label": "Owner", "value": "Not set"},
        ],
        columns=4,
    )

    tabs = st.tabs(["Column Details", "AI Explanation", "Sample SQL", "Related Tables", "Data Quality"])

    with tabs[0]:
        try:
            columns_df = run_query(
                sql_information_schema_columns(catalog, meta_row["schema_name"], table_name)
            )
            render_dataframe(columns_df, empty_message="No column metadata available.")
        except (DatabricksConnectionError, DatabricksQueryError) as exc:
            st.error(f"Could not load column details: {exc}")

    with tabs[1]:
        try:
            analysis_df = run_query(sql_ai_analysis(catalog=catalog, table_name=table_name))
        except (DatabricksConnectionError, DatabricksQueryError) as exc:
            st.error(f"Could not load AI analysis: {exc}")
            analysis_df = None

        if analysis_df is not None and not analysis_df.empty:
            latest = analysis_df.iloc[0]
            col1, col2 = st.columns(2)
            col1.metric("Health Score", latest.get("health_score", "—"))
            col2.metric("Confidence Score", latest.get("confidence_score", "—"))
            st.markdown(latest.get("analysis_markdown") or "_No content stored._")
            st.caption(f"Last Refresh: {latest.get('analysis_timestamp', '—')}")
        else:
            render_empty_state(
                f"No AI analysis stored yet for '{table_name}'. Run "
                "03_metadata_analyzer_poc.py for this table.",
                icon="🤖",
            )
            if st.button("Ask Gemini for a quick explanation instead", key=f"quick_explain_{table_name}"):
                with st.spinner("Asking Gemini..."):
                    try:
                        prompt = (
                            f"In 2-3 sentences, explain the likely business purpose of a "
                            f"warehouse table named '{table_name}' in an AdventureWorks-based "
                            f"sales data warehouse. Be clear this is a general inference, not "
                            f"a fact about this specific table's actual content."
                        )
                        st.info(generate_content(prompt))
                    except GeminiConfigurationError:
                        st.error("Gemini isn't configured yet — see ⚙ Settings.")
                    except GeminiClientError as exc:
                        st.error(f"Could not generate an explanation: {exc}")

    with tabs[2]:
        st.code(f"SELECT * FROM {catalog}.{meta_row['schema_name']}.{table_name} LIMIT 100", language="sql")

    with tabs[3]:
        related = _related_tables(table_name)
        if related:
            for t in related:
                st.markdown(f"- `{t}`")
        else:
            render_empty_state("No known relationships for this table.", icon="🔗")

    with tabs[4]:
        pk = to_plain_list(meta_row.get("primary_key_candidates"))
        null_pct_values = [
            col["null_percentage"]
            for col in parse_nested_field(meta_row.get("columns"))
            if col.get("null_percentage") is not None
        ]
        avg_null_pct = sum(null_pct_values) / len(null_pct_values) if null_pct_values else None

        render_metric_grid(
            [
                {"label": "Primary Key Candidates", "value": ", ".join(pk) if pk else "—"},
                {
                    "label": "Avg Column Null %",
                    "value": format_pct(avg_null_pct / 100 if avg_null_pct is not None else None),
                },
                {"label": "Tags", "value": "Not set"},
            ],
            columns=3,
        )


metadata_df = None
with st.spinner("Loading table catalog..."):
    try:
        metadata_df = run_query(sql_table_metadata(catalog=catalog))
    except (DatabricksConnectionError, DatabricksQueryError) as exc:
        st.error(f"Could not load the data catalog: {exc}")

if metadata_df is None:
    render_empty_state(
        "Connect to Databricks (see ⚙ Settings) to browse "
        f"`{catalog}.metadata.table_metadata`.",
        icon="🔌",
    )
elif metadata_df.empty:
    render_empty_state(
        "table_metadata has no rows yet — run 02_metadata_collector.py first.", icon="📭"
    )
else:
    col1, col2, col3 = st.columns([2, 1, 1])
    with col1:
        search = st.text_input("🔍 Search tables", placeholder="e.g. customer")
    with col2:
        layer_options = ["All"] + sorted(metadata_df["schema_name"].dropna().unique().tolist())
        layer_filter = st.selectbox("Layer", layer_options)
    with col3:
        sort_by = st.selectbox("Sort by", ["table_name", "row_count", "column_count"])

    filtered_df = metadata_df
    if search:
        filtered_df = filtered_df[filtered_df["table_name"].str.contains(search, case=False, na=False)]
    if layer_filter != "All":
        filtered_df = filtered_df[filtered_df["schema_name"] == layer_filter]
    filtered_df = filtered_df.sort_values(sort_by)

    st.caption(f"{len(filtered_df)} of {len(metadata_df)} tables")
    display_columns = ["catalog_name", "schema_name", "table_name", "column_count", "row_count"]
    render_dataframe(filtered_df[display_columns] if not filtered_df.empty else filtered_df)

    table_names = sorted(filtered_df["table_name"].dropna().unique().tolist())
    if table_names:
        st.divider()
        selected_table = st.selectbox("View details for", table_names)
        selected_row = filtered_df[filtered_df["table_name"] == selected_table].iloc[0]
        _render_table_detail(selected_table, selected_row)
