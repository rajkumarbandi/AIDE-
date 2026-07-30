"""AI Assistant — a general-purpose Gemini chat about the AIDE warehouse.

Foundation scope: a real, working chat (Gemini-backed) — but not yet
warehouse-aware/tool-using (it can't query the warehouse on your behalf or
ground its answers in live data). That's the natural next phase, once this
chat shell exists to build on.
"""

import streamlit as st

from components.chat import render_chat_history, render_chat_input
from components.header import render_page_header
from utils.gemini import GeminiClientError, GeminiConfigurationError, generate_content

render_page_header(
    title="AI Assistant",
    description="Ask general questions about data engineering, the medallion "
    "architecture, or this project — powered by Gemini.",
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

render_chat_history(_HISTORY_KEY)


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


render_chat_input(_HISTORY_KEY, placeholder="Ask AIDE anything...", on_user_message=_ask_gemini)

if not st.session_state.get(_HISTORY_KEY):
    st.caption(
        '💡 Try: "What is a medallion architecture?" or "Why does Silver drop rowguid columns?"'
    )
