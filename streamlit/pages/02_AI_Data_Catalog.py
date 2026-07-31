"""AI Data Catalog — search, filter, and sort every discovered table across
Bronze, Silver, and Gold, with a full detail view (columns, AI explanation,
sample SQL, related tables, data quality metrics) on selection.

Honesty note on scope: 02_metadata_collector.py currently only AI-analyzes
the Bronze layer, so Bronze rows carry real, rich collected metadata (row
count, column profile, PK candidates) while Silver/Gold rows are built live
from information_schema — real, live metadata, just not the same
AI-generated business analysis Bronze has yet. Row count for Silver/Gold is
fetched on demand for the selected table only, never eagerly for every row
in the listing (see sql_row_count's docstring).

"Tags" and "Owner" are not fields that exist anywhere in metadata.table_metadata
or metadata.ai_analysis — there is no real data source for them yet, so
they're shown as genuinely empty ("Not set"), never fabricated. "Source
System" is shown as "AdventureWorks" because that's a true fact about this
specific warehouse, not an invented value.
"""

import pandas as pd
import streamlit as st

from components.filters import get_filters
from components.header import render_page_header
from components.metric_cards import layer_badge, render_metric_grid
from components.shell import render_app_shell
from components.tables import render_dataframe, render_empty_state
from utils.config import DEFAULT_GOLD_SCHEMA, DEFAULT_METADATA_SCHEMA, DEFAULT_SILVER_SCHEMA
from utils.databricks import DatabricksConnectionError, DatabricksQueryError, run_query
from utils.gemini import GeminiClientError, GeminiConfigurationError, generate_content
from utils.governance import PRIORITIES, GovernanceError, load_comments, submit_comment
from utils.helpers import format_number, format_pct, parse_nested_field, to_plain_list
from utils.queries import (
    GOLD_LINEAGE,
    GOLD_RELATIONSHIPS,
    SILVER_LINEAGE,
    gold_keys_for_table,
    sql_ai_analysis,
    sql_information_schema_columns,
    sql_information_schema_tables,
    sql_row_count,
    sql_schema_table_column_counts,
    sql_table_metadata,
)

render_app_shell()

render_page_header(
    title="AI Data Catalog",
    description="Search, filter, and explore every discovered table's metadata and "
    "AI-generated business analysis across Bronze, Silver, and Gold.",
    breadcrumb=["Home", "AI Data Catalog"],
)

filters = get_filters()
catalog = filters["catalog"]


def _safe_query(sql: str):
    try:
        return run_query(sql)
    except (DatabricksConnectionError, DatabricksQueryError) as exc:
        st.session_state.setdefault("_catalog_errors", []).append(str(exc))
        return None


@st.cache_data(ttl=300, show_spinner=False)
def _load_unified_catalog(catalog: str) -> pd.DataFrame:
    """Bronze rows from the AI-collected metadata.table_metadata (real, rich
    data); Silver/Gold rows built live from information_schema (real, live,
    just not yet AI-analyzed — see module docstring).
    """
    rows = []

    bronze_df = _safe_query(sql_table_metadata(catalog=catalog))
    if bronze_df is not None:
        for _, r in bronze_df.iterrows():
            rows.append(
                {
                    "layer": r.get("schema_name", "bronze"),
                    "table_name": r["table_name"],
                    "column_count": r.get("column_count"),
                    "row_count": r.get("row_count"),
                    "primary_key_candidates": r.get("primary_key_candidates"),
                    "columns": r.get("columns"),
                    "has_ai_analysis": True,
                }
            )

    for layer, schema in [("silver", DEFAULT_SILVER_SCHEMA), ("gold", DEFAULT_GOLD_SCHEMA)]:
        tables_df = _safe_query(sql_information_schema_tables(catalog, schema))
        if tables_df is None or tables_df.empty:
            continue
        counts_df = _safe_query(sql_schema_table_column_counts(catalog, schema))
        counts_by_table = (
            dict(zip(counts_df["table_name"], counts_df["column_count"]))
            if counts_df is not None
            else {}
        )
        for _, r in tables_df.iterrows():
            rows.append(
                {
                    "layer": layer,
                    "table_name": r["table_name"],
                    "column_count": counts_by_table.get(r["table_name"]),
                    "row_count": None,
                    "primary_key_candidates": None,
                    "columns": None,
                    "has_ai_analysis": False,
                }
            )

    return pd.DataFrame(rows)


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


def _render_table_detail(table_name: str, layer: str, meta_row: pd.Series) -> None:
    st.subheader(f"📄 {table_name.replace('_', ' ').title()}")
    st.markdown(
        f"Business Name: {table_name.replace('_', ' ').title()} · {layer_badge(layer)} · "
        "Source System: AdventureWorks",
        unsafe_allow_html=True,
    )

    schema_name = layer
    row_count = meta_row.get("row_count")
    if row_count is None or pd.isna(row_count):
        row_count_df = _safe_query(sql_row_count(catalog, schema_name, table_name))
        row_count = row_count_df.iloc[0]["row_count"] if row_count_df is not None else None

    render_metric_grid(
        [
            {"label": "Layer", "value": layer.title()},
            {"label": "Row Count", "value": format_number(row_count)},
            {"label": "Columns", "value": str(meta_row.get("column_count") or "—")},
            {"label": "Owner", "value": "Not set"},
        ],
        columns=4,
    )
    if not meta_row.get("has_ai_analysis"):
        st.caption(
            "ℹ This table hasn't been AI-analyzed yet (only Bronze is currently scanned by "
            "02_metadata_collector.py) — metadata above is live from information_schema."
        )

    tabs = st.tabs(
        ["Column Details", "AI Explanation", "Sample SQL", "Related Tables", "Data Quality",
         "💬 Reviewer Comments"]
    )

    with tabs[0]:
        columns_df = _safe_query(sql_information_schema_columns(catalog, schema_name, table_name))
        render_dataframe(columns_df, empty_message="No column metadata available.")
        st.caption(
            "Need null %, distinct %, sample values, or a value distribution for a column? "
            "See 🗄 Warehouse Explorer's Column Profiling tab."
        )

    with tabs[1]:
        analysis_df = _safe_query(sql_ai_analysis(catalog=catalog, table_name=table_name))

        if analysis_df is not None and not analysis_df.empty:
            latest = analysis_df.iloc[0]
            col1, col2 = st.columns(2)
            col1.metric("Health Score", latest.get("health_score", "—"))
            col2.metric("Confidence Score", latest.get("confidence_score", "—"))
            st.markdown(latest.get("analysis_markdown") or "_No content stored._")
            st.caption(f"Last Refresh: {latest.get('analysis_timestamp', '—')}")
        else:
            render_empty_state(
                f"No AI analysis stored yet for '{table_name}'{' (' + layer + ' layer)' if layer != 'bronze' else ''}.",
                icon="🤖",
            )
            if st.button("Ask Gemini for a quick explanation instead", key=f"quick_explain_{table_name}"):
                with st.spinner("Asking Gemini..."):
                    try:
                        prompt = (
                            f"In 2-3 sentences, explain the likely business purpose of a "
                            f"{layer}-layer warehouse table named '{table_name}' in an "
                            f"AdventureWorks-based sales data warehouse. Be clear this is a "
                            f"general inference, not a fact about this specific table's actual content."
                        )
                        st.info(generate_content(prompt))
                    except GeminiConfigurationError:
                        st.error("Gemini isn't configured yet — see ⚙ Settings.")
                    except GeminiClientError as exc:
                        st.error(f"Could not generate an explanation: {exc}")

    with tabs[2]:
        st.code(f"SELECT * FROM {catalog}.{schema_name}.{table_name} LIMIT 100", language="sql")

    with tabs[3]:
        related = _related_tables(table_name)
        if related:
            for t in related:
                st.markdown(f"- `{t}`")
        else:
            render_empty_state("No known relationships for this table.", icon="🔗")

    with tabs[4]:
        if layer == "gold":
            primary_keys, foreign_keys = gold_keys_for_table(table_name)
        else:
            primary_keys, foreign_keys = to_plain_list(meta_row.get("primary_key_candidates")), []

        null_pct_values = [
            col["null_percentage"]
            for col in parse_nested_field(meta_row.get("columns"))
            if col.get("null_percentage") is not None
        ]
        avg_null_pct = sum(null_pct_values) / len(null_pct_values) if null_pct_values else None

        render_metric_grid(
            [
                {"label": "Primary Key(s)", "value": ", ".join(primary_keys) if primary_keys else "—"},
                {"label": "Foreign Key(s)", "value": "; ".join(foreign_keys) if foreign_keys else "—"},
                {
                    "label": "Avg Column Null %",
                    "value": format_pct(avg_null_pct / 100 if avg_null_pct is not None else None),
                },
                {"label": "Tags", "value": "Not set"},
            ],
            columns=4,
        )

    with tabs[5]:
        _render_comments_tab(table_name)


def _render_comments_tab(table_name: str) -> None:
    """Submit a reviewer comment about this table, and see its existing
    comments/status — the full review workflow (approve/reject/assign) lives
    on the dedicated 🛡 Data Governance page; this tab is for raising and
    tracking issues from the table you're actually looking at.
    """
    st.caption(
        "Flag incorrect metadata, suggest a better description, report a wrong relationship "
        "or profiling figure, or request documentation — every comment is tracked with a "
        "status on the 🛡 Data Governance page."
    )

    with st.form(key=f"comment_form_{table_name}", clear_on_submit=True):
        col1, col2 = st.columns(2)
        with col1:
            reason = st.selectbox(
                "Reason",
                [
                    "Flag incorrect metadata", "Suggest better description",
                    "Report incorrect relationship", "Report incorrect profiling",
                    "Suggest naming improvement", "Request documentation update", "Other",
                ],
                key=f"reason_{table_name}",
            )
            affected_column = st.text_input(
                "Affected column (optional)", key=f"col_{table_name}",
                placeholder="leave blank if table-level",
            )
        with col2:
            priority = st.selectbox("Priority", PRIORITIES, index=1, key=f"priority_{table_name}")
            author = st.text_input("Your name", key=f"author_{table_name}", placeholder="e.g. Jane Doe")

        comment_text = st.text_area("Comment", key=f"comment_{table_name}")
        suggested_change = st.text_area(
            "Suggested change (optional)", key=f"suggestion_{table_name}"
        )

        if st.form_submit_button("Submit Comment"):
            if not comment_text.strip() or not author.strip():
                st.warning("A comment and your name are both required.")
            else:
                try:
                    submit_comment(
                        catalog, DEFAULT_METADATA_SCHEMA, table_name, comment_text.strip(),
                        author.strip(), priority=priority,
                        affected_column=affected_column.strip() or None,
                        reason=reason, suggested_change=suggested_change.strip() or None,
                    )
                    st.success("Comment submitted — see it on the 🛡 Data Governance page.")
                except GovernanceError as exc:
                    st.error(str(exc))

    st.divider()
    try:
        comments_df = load_comments(catalog, DEFAULT_METADATA_SCHEMA, affected_table=table_name)
    except GovernanceError as exc:
        st.error(str(exc))
        comments_df = None

    if comments_df is None or comments_df.empty:
        render_empty_state("No reviewer comments yet for this table.", icon="💬")
    else:
        st.caption(f"{len(comments_df)} comment(s) for this table")
        for _, row in comments_df.iterrows():
            with st.container(border=True):
                st.markdown(
                    f"**{row['reason']}** · {row['status']} · Priority: {row['priority']} · "
                    f"{row['author']} · {row['created_at']}"
                )
                st.markdown(row["comment_text"])
                if row.get("affected_column"):
                    st.caption(f"Column: `{row['affected_column']}`")
                if row.get("suggested_change"):
                    st.caption(f"Suggested change: {row['suggested_change']}")


with st.spinner("Loading table catalog across Bronze, Silver, and Gold..."):
    catalog_df = _load_unified_catalog(catalog)

if catalog_df.empty:
    render_empty_state(
        "Connect to Databricks (see ⚙ Settings) to browse the catalog, or run "
        "02_metadata_collector.py if Bronze metadata hasn't been collected yet.",
        icon="🔌",
    )
else:
    col1, col2, col3 = st.columns([2, 1, 1])
    with col1:
        search = st.text_input("🔍 Search tables", placeholder="e.g. customer")
    with col2:
        layer_options = ["All"] + sorted(catalog_df["layer"].dropna().unique().tolist())
        layer_filter = st.selectbox("Layer", layer_options)
    with col3:
        sort_by = st.selectbox("Sort by", ["table_name", "layer", "column_count"])

    filtered_df = catalog_df
    if search:
        filtered_df = filtered_df[filtered_df["table_name"].str.contains(search, case=False, na=False)]
    if layer_filter != "All":
        filtered_df = filtered_df[filtered_df["layer"] == layer_filter]
    filtered_df = filtered_df.sort_values(sort_by)

    st.caption(f"{len(filtered_df)} of {len(catalog_df)} tables across Bronze, Silver, and Gold")
    display_columns = ["layer", "table_name", "column_count", "row_count", "has_ai_analysis"]
    render_dataframe(filtered_df[display_columns] if not filtered_df.empty else filtered_df)

    table_names = sorted(filtered_df["table_name"].dropna().unique().tolist())
    if table_names:
        st.divider()
        selected_table = st.selectbox("View details for", table_names)
        selected_row = filtered_df[filtered_df["table_name"] == selected_table].iloc[0]
        _render_table_detail(selected_table, selected_row["layer"], selected_row)
