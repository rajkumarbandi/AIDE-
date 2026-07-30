"""SQL Playground — run ad-hoc SQL against the warehouse with syntax
highlighting, execution timing, CSV export, saved queries, and query
history. Only a single, plain SELECT statement may run — validate_select_only
hard-blocks DROP/DELETE/TRUNCATE/UPDATE/INSERT/etc. (including inside nested
subqueries), so this stays a genuinely read-only surface.

Saved queries and history are session-scoped (st.session_state) — there is no
persistence layer for them yet, so they reset when the browser session ends.
That's stated plainly here rather than implying a durable "save" that isn't real.
"""

import datetime

import streamlit as st

from components.filters import get_filters
from components.header import render_page_header
from components.sql_editor import render_sql_editor, set_sql_editor_value
from components.tables import render_dataframe, render_empty_state
from utils.databricks import (
    DatabricksConnectionError,
    DatabricksQueryError,
    UnsafeQueryError,
    run_query_timed,
    validate_select_only,
)
from utils.helpers import truncate_text

render_page_header(
    title="SQL Playground",
    description="Run read-only SQL directly against the warehouse — with syntax "
    "highlighting, timing, history, and saved queries.",
    breadcrumb=["Home", "SQL Playground"],
)

_EDITOR_KEY = "aide_playground_editor"
_HISTORY_KEY = "aide_playground_history"
_SAVED_KEY = "aide_playground_saved"
_MAX_HISTORY = 20

filters = get_filters()
default_query = (
    f"SELECT * FROM {filters['catalog']}.{filters['schema']}.sales_kpi_summary LIMIT 10"
)

current_sql = render_sql_editor(
    st.session_state.get(_EDITOR_KEY, default_query), key=_EDITOR_KEY, height=180
)

col_run, col_save_name, col_save_btn = st.columns([1, 2, 1])
with col_run:
    run_clicked = st.button("▶ Run Query", type="primary", key="aide_playground_run")
with col_save_name:
    save_name = st.text_input(
        "Save as", placeholder="e.g. Top territories by revenue",
        key="aide_playground_save_name", label_visibility="collapsed",
    )
with col_save_btn:
    save_clicked = st.button("💾 Save Query", key="aide_playground_save")

st.caption("⚠ Only a single SELECT statement may run — DROP/DELETE/TRUNCATE/UPDATE/INSERT/"
           "ALTER/CREATE/MERGE and stacked queries are blocked, even inside subqueries.")

if save_clicked:
    if not current_sql.strip():
        st.warning("Nothing to save — the editor is empty.")
    elif not save_name.strip():
        st.warning("Enter a name for this saved query first.")
    else:
        saved = st.session_state.setdefault(_SAVED_KEY, [])
        saved[:] = [s for s in saved if s["name"] != save_name.strip()]
        saved.insert(0, {"name": save_name.strip(), "sql": current_sql})
        st.success(f"Saved as \"{save_name.strip()}\".")

if run_clicked:
    if not current_sql.strip():
        st.warning("Enter a query first.")
    else:
        try:
            validate_select_only(current_sql)
            with st.spinner("Running query..."):
                result_df, elapsed = run_query_timed(current_sql)
            st.success(f"⏱ {elapsed:.2f}s · {len(result_df)} row(s) returned.")
            render_dataframe(result_df, empty_message="Query ran successfully but returned no rows.")
            if not result_df.empty:
                st.download_button(
                    "⬇ Download as CSV",
                    data=result_df.to_csv(index=False),
                    file_name="sql_playground_result.csv",
                    mime="text/csv",
                    key="aide_playground_download",
                )

            history = st.session_state.setdefault(_HISTORY_KEY, [])
            history.insert(
                0,
                {
                    "sql": current_sql,
                    "timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "row_count": len(result_df),
                    "elapsed": elapsed,
                },
            )
            del history[_MAX_HISTORY:]
        except UnsafeQueryError as exc:
            st.error(f"Query blocked: {exc}")
        except (DatabricksConnectionError, DatabricksQueryError) as exc:
            st.error(f"Query failed: {exc}")

st.divider()
history_tab, saved_tab = st.tabs(["📜 Query History", "⭐ Saved Queries"])

with history_tab:
    history = st.session_state.get(_HISTORY_KEY, [])
    if not history:
        render_empty_state("No queries run yet this session.", icon="📜")
    else:
        for i, entry in enumerate(history):
            col_text, col_load = st.columns([5, 1])
            with col_text:
                st.markdown(
                    f"`{truncate_text(entry['sql'], 90)}`  \n"
                    f"<span style='color:var(--aide-text-muted);font-size:12px;'>"
                    f"{entry['timestamp']} · {entry['row_count']} row(s) · "
                    f"{entry['elapsed']:.2f}s</span>",
                    unsafe_allow_html=True,
                )
            with col_load:
                if st.button("Load", key=f"aide_history_load_{i}"):
                    set_sql_editor_value(_EDITOR_KEY, entry["sql"])
                    st.rerun()

with saved_tab:
    saved = st.session_state.get(_SAVED_KEY, [])
    if not saved:
        render_empty_state("No saved queries yet — use \"Save Query\" above.", icon="⭐")
    else:
        for i, entry in enumerate(saved):
            col_text, col_load, col_delete = st.columns([4, 1, 1])
            with col_text:
                st.markdown(f"**{entry['name']}**  \n`{truncate_text(entry['sql'], 80)}`")
            with col_load:
                if st.button("Load", key=f"aide_saved_load_{i}"):
                    set_sql_editor_value(_EDITOR_KEY, entry["sql"])
                    st.rerun()
            with col_delete:
                if st.button("🗑", key=f"aide_saved_delete_{i}"):
                    saved.pop(i)
                    st.rerun()
