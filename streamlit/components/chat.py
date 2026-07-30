"""Reusable chat UI primitives, built on Streamlit's native chat elements
(st.chat_message / st.chat_input) — no custom chat widget needed.

Each page using this owns its own session_state history key, so the AI
Assistant page and the AI Data Model Explorer's "Explain this table" panel
(if it grows a conversational follow-up later) don't share history.
"""

from typing import Callable, Optional

import streamlit as st


def render_chat_history(history_key: str) -> None:
    """Render every message currently in st.session_state[history_key]."""
    for message in st.session_state.get(history_key, []):
        with st.chat_message(message["role"]):
            st.markdown(message["content"])


def append_message(history_key: str, role: str, content: str) -> None:
    st.session_state.setdefault(history_key, []).append({"role": role, "content": content})


def render_chat_input(
    history_key: str,
    placeholder: str,
    on_user_message: Callable[[str], Optional[str]],
) -> None:
    """Render the chat input box; on submit, append the user message, call
    `on_user_message(text)` to get the assistant's reply, and append that too.

    `on_user_message` should handle its own errors and return an error-state
    message string rather than raising — this component doesn't wrap it in a
    try/except, since what counts as a recoverable error is page-specific.
    """
    user_input = st.chat_input(placeholder)
    if not user_input:
        return
    append_message(history_key, "user", user_input)
    with st.chat_message("user"):
        st.markdown(user_input)

    reply = on_user_message(user_input)
    if reply:
        append_message(history_key, "assistant", reply)
        with st.chat_message("assistant"):
            st.markdown(reply)
