"""AI Assistant — general-purpose Gemini chat about the AIDE warehouse, plus
a full natural-language-to-SQL workflow: ask a question about the data,
Gemini drafts a SELECT grounded in the live schema, edit it before running,
execute it for real, and get a plain-language explanation.

The two modes are separate tabs, not a merge — the existing chat (Phase 1,
working) is untouched; "Ask About Your Data" is additive.
"""

import streamlit as st

from components.chat import render_chat_history, render_chat_input
from components.filters import get_filters
from components.header import render_page_header
from components.sql_editor import render_sql_editor
from components.tables import render_dataframe, render_empty_state
from utils.databricks import (
    DatabricksConnectionError,
    DatabricksQueryError,
    UnsafeQueryError,
    run_query,
    run_query_timed,
    validate_select_only,
)
from utils.gemini import (
    GeminiClientError,
    GeminiConfigurationError,
    explain_sql,
    generate_content,
    generate_sql_from_question,
)
from utils.queries import sql_schema_columns

render_page_header(
    title="AI Assistant",
    description="Chat about data engineering concepts, or ask a question about the "
    "warehouse data and let Gemini draft a SQL query you can review and run.",
    breadcrumb=["Home", "AI Assistant"],
)

_HISTORY_KEY = "aide_assistant_history"
_SYSTEM_PROMPT = (
    "You are AIDE's AI Assistant, embedded in an enterprise data engineering "
    "platform built on Databricks, Delta Lake, and a medallion (Bronze/Silver/"
    "Gold) architecture over the AdventureWorks dataset. Answer concisely and "
    "helpfully. You do not yet have live access to the warehouse's data — if "
    "asked for specific current numbers, say so rather than guessing."
)


def _ask_gemini(user_text: str) -> str:
    history = st.session_state.get(_HISTORY_KEY, [])
    conversation = "\n".join(f"{m['role']}: {m['content']}" for m in history[-10:])
    prompt = f"{_SYSTEM_PROMPT}\n\nConversation so far:\n{conversation}"
    try:
        return generate_content(prompt)
    except GeminiConfigurationError:
        return (
            "⚠ Gemini isn't configured yet — add `[gemini] api_key` to "
            "`.streamlit/secrets.toml` (see ⚙ Settings)."
        )
    except (GeminiClientError, ValueError) as exc:
        return f"⚠ I couldn't get a response from Gemini: {exc}"


@st.cache_data(ttl=300, show_spinner=False)
def _load_schema_context(catalog: str, schema: str) -> str:
    """Live table.column list for the current schema — the real grounding
    text passed to Gemini so it can't invent a table/column that doesn't
    exist. Empty string (not fabricated placeholder text) if unreachable.
    """
    try:
        df = run_query(sql_schema_columns(catalog, schema))
    except (DatabricksConnectionError, DatabricksQueryError):
        return ""
    if df.empty:
        return ""
    lines = [
        f"{catalog}.{schema}.{row['table_name']}.{row['column_name']} ({row['data_type']})"
        for _, row in df.iterrows()
    ]
    return "\n".join(lines)


chat_tab, sql_tab = st.tabs(["💬 General Chat", "🗄 Ask About Your Data"])

with chat_tab:
    render_chat_history(_HISTORY_KEY)
    render_chat_input(_HISTORY_KEY, placeholder="Ask AIDE anything...", on_user_message=_ask_gemini)
    if not st.session_state.get(_HISTORY_KEY):
        st.caption(
            '💡 Try: "What is a medallion architecture?" or "Why does Silver drop '
            'rowguid columns?"'
        )

with sql_tab:
    filters = get_filters()
    catalog, schema = filters["catalog"], filters["schema"]

    question = st.text_input(
        "Ask a question about the data",
        placeholder="e.g. What were total sales by territory last year?",
        key="aide_nl_question",
    )

    if st.button("✨ Generate SQL", key="aide_generate_sql_btn"):
        if not question.strip():
            st.warning("Enter a question first.")
        else:
            schema_context = _load_schema_context(catalog, schema)
            if not schema_context:
                st.error(
                    f"Could not load the live schema for `{catalog}.{schema}` — connect to "
                    "Databricks (see ⚙ Settings) before generating SQL."
                )
            else:
                with st.spinner("Asking Gemini to draft a SQL query..."):
                    try:
                        st.session_state["aide_generated_sql"] = generate_sql_from_question(
                            question, schema_context
                        )
                    except GeminiConfigurationError:
                        st.error("Gemini isn't configured yet — see ⚙ Settings.")
                    except GeminiClientError as exc:
                        st.error(f"Could not generate SQL: {exc}")

    if st.session_state.get("aide_generated_sql"):
        st.caption("Review and edit the generated SQL before running it — it will not run "
                   "automatically.")
        edited_sql = render_sql_editor(
            st.session_state["aide_generated_sql"], key="aide_nl_sql_editor"
        )

        col_run, col_explain = st.columns(2)
        with col_run:
            run_clicked = st.button("▶ Run Query", key="aide_nl_run_btn")
        with col_explain:
            explain_clicked = st.button("🤖 Explain this SQL", key="aide_nl_explain_btn")

        if run_clicked:
            try:
                validate_select_only(edited_sql)
                with st.spinner("Running query..."):
                    result_df, elapsed = run_query_timed(edited_sql)
                st.caption(f"⏱ {elapsed:.2f}s · {len(result_df)} row(s)")
                render_dataframe(result_df, empty_message="Query ran successfully but returned no rows.")
                if not result_df.empty:
                    st.download_button(
                        "⬇ Download as CSV",
                        data=result_df.to_csv(index=False),
                        file_name="ai_assistant_query_result.csv",
                        mime="text/csv",
                        key="aide_nl_download",
                    )
            except UnsafeQueryError as exc:
                st.error(f"Query blocked: {exc}")
            except (DatabricksConnectionError, DatabricksQueryError) as exc:
                st.error(f"Query failed: {exc}")

        if explain_clicked:
            with st.spinner("Asking Gemini to explain the query..."):
                try:
                    st.info(explain_sql(edited_sql))
                except GeminiConfigurationError:
                    st.error("Gemini isn't configured yet — see ⚙ Settings.")
                except GeminiClientError as exc:
                    st.error(f"Could not explain the query: {exc}")
    else:
        render_empty_state(
            "Ask a question above and click \"Generate SQL\" to get started.", icon="🗄"
        )
