"""AI Assistant — a single, ChatGPT-style chat. Ask anything: a general
question, or a question about the warehouse's live data.

Behind the scenes, utils.agent.answer_question() decides which kind of
question it is, and for data questions, silently builds warehouse context,
generates SQL, validates it, executes it, and turns the real result into a
business-friendly answer. There is no SQL editor, no "Generate SQL"/"Run
Query" button, and no manual review step — by explicit design. An optional
debug expander reveals the SQL and raw result behind the most recent answer,
for anyone who wants to verify what actually ran; it is never shown by default.
"""

import streamlit as st

from components.chat import render_chat_history, render_chat_input
from components.filters import get_filters
from components.header import render_page_header
from components.shell import render_app_shell
from components.tables import render_dataframe
from utils.agent import answer_question

render_app_shell()

render_page_header(
    title="AI Assistant",
    description="Ask anything about the business — the assistant answers from live "
    "warehouse data automatically. No SQL, no buttons, just an answer.",
    breadcrumb=["Home", "AI Assistant"],
)

_HISTORY_KEY = "aide_assistant_history"
_DEBUG_KEY = "aide_assistant_last_debug"

filters = get_filters()


def _handle_question(question: str) -> str:
    with st.spinner("Analyzing your question..."):
        result = answer_question(question, catalog=filters["catalog"], gold_schema=filters["schema"])
    st.session_state[_DEBUG_KEY] = result
    return result.answer


render_chat_history(_HISTORY_KEY)
render_chat_input(
    _HISTORY_KEY,
    placeholder="Ask about your business, e.g. \"Top 10 customers by revenue\"...",
    on_user_message=_handle_question,
)

if not st.session_state.get(_HISTORY_KEY):
    st.caption(
        '💡 Try: "How many rows are in fact_sales?", "Top 10 customers by revenue", '
        '"Which territory generated the highest revenue?", or "What is a medallion architecture?"'
    )

last_debug = st.session_state.get(_DEBUG_KEY)
if last_debug is not None:
    with st.expander("🔍 Debug: view the SQL and raw result behind the last answer"):
        if last_debug.sql:
            st.code(last_debug.sql, language="sql")
            render_dataframe(last_debug.result_df, empty_message="Query returned no rows.")
        else:
            st.caption("No SQL was generated for the last message — it wasn't a data question.")
        if len(last_debug.attempts) > 1:
            st.caption(f"Took {len(last_debug.attempts)} attempt(s) to get a working query.")
        if last_debug.error:
            st.caption(f"Internal note: {last_debug.error}")
