"""Data Governance — review every reviewer comment raised across the
catalog, with filters, search, sorting, and a full status workflow (New →
Flagged/Under Review → Approved/Rejected/Resolved/Implemented), plus a
reply thread per comment for an audit trail.

Comments are submitted from the AI Data Catalog's per-table "Reviewer
Comments" tab; this page is where a data owner reviews and actions all of
them in one place — the enterprise governance workflow itself.

Honesty note: this app has no authentication system, so "author"/"reviewer"
is a self-reported name typed into a text box, not a verified identity —
stated plainly rather than implying real access control that doesn't exist.
"""

import html

import streamlit as st

from components.filters import get_filters
from components.header import render_page_header
from components.metric_cards import priority_badge, status_badge
from components.shell import render_app_shell
from components.tables import render_empty_state
from utils.config import DEFAULT_METADATA_SCHEMA
from utils.governance import (
    OPEN_STATUSES,
    PRIORITIES,
    STATUSES,
    GovernanceError,
    add_reply,
    load_comments,
    load_replies,
    update_comment_status,
)

render_app_shell()

render_page_header(
    title="Data Governance",
    description="Review, filter, and action every reviewer comment raised across the catalog — "
    "an enterprise data governance workflow for metadata quality.",
    breadcrumb=["Home", "Data Governance"],
)

filters = get_filters()
catalog = filters["catalog"]

reviewer_name = st.text_input(
    "Reviewing as", placeholder="e.g. Jane Doe (data owner)", key="aide_governance_reviewer"
)

try:
    comments_df = load_comments(catalog, DEFAULT_METADATA_SCHEMA)
except GovernanceError as exc:
    st.error(str(exc))
    comments_df = None

if comments_df is None:
    render_empty_state("Connect to Databricks (see ⚙ Settings) to load governance comments.", icon="🔌")
elif comments_df.empty:
    render_empty_state(
        "No reviewer comments yet — raise one from any table's 💬 Reviewer Comments tab on "
        "the AI Data Catalog page.",
        icon="🛡",
    )
else:
    open_count = int(comments_df["status"].isin(OPEN_STATUSES).sum())
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Comments", len(comments_df))
    col2.metric("Open", open_count)
    col3.metric("Resolved / Implemented", len(comments_df) - open_count)

    st.divider()
    col_search, col_status, col_priority, col_table, col_sort = st.columns([2, 1, 1, 1, 1])
    with col_search:
        search = st.text_input("🔍 Search comments", placeholder="e.g. territory")
    with col_status:
        status_filter = st.selectbox("Status", ["All"] + STATUSES)
    with col_priority:
        priority_filter = st.selectbox("Priority", ["All"] + PRIORITIES)
    with col_table:
        table_options = ["All"] + sorted(comments_df["affected_table"].dropna().unique().tolist())
        table_filter = st.selectbox("Table", table_options)
    with col_sort:
        sort_by = st.selectbox("Sort by", ["created_at", "priority", "status", "affected_table"])

    filtered_df = comments_df
    if search:
        mask = (
            filtered_df["comment_text"].str.contains(search, case=False, na=False)
            | filtered_df["affected_table"].str.contains(search, case=False, na=False)
            | filtered_df["reason"].str.contains(search, case=False, na=False)
        )
        filtered_df = filtered_df[mask]
    if status_filter != "All":
        filtered_df = filtered_df[filtered_df["status"] == status_filter]
    if priority_filter != "All":
        filtered_df = filtered_df[filtered_df["priority"] == priority_filter]
    if table_filter != "All":
        filtered_df = filtered_df[filtered_df["affected_table"] == table_filter]
    filtered_df = filtered_df.sort_values(sort_by, ascending=(sort_by != "created_at"))

    st.caption(f"{len(filtered_df)} of {len(comments_df)} comment(s)")

    if filtered_df.empty:
        render_empty_state("No comments match the current filters.", icon="🔍")
    else:
        for _, row in filtered_df.iterrows():
            comment_id = row["comment_id"]
            with st.container(border=True):
                st.markdown(
                    f"**{row['affected_table']}**"
                    + (f" · `{row['affected_column']}`" if row.get("affected_column") else "")
                    + f" — {row['reason']}"
                )
                st.markdown(
                    f"{status_badge(row['status'])} {priority_badge(row['priority'])} · "
                    f"By {html.escape(str(row['author']))} on {row['created_at']} · "
                    f"Last updated {row['updated_at']}",
                    unsafe_allow_html=True,
                )
                st.markdown(row["comment_text"])
                if row.get("suggested_change"):
                    st.markdown(f"**Suggested change:** {row['suggested_change']}")

                col_status_change, col_reply = st.columns([1, 2])
                with col_status_change:
                    new_status = st.selectbox(
                        "Change status", STATUSES,
                        index=STATUSES.index(row["status"]) if row["status"] in STATUSES else 0,
                        key=f"status_select_{comment_id}",
                    )
                    if new_status != row["status"] and st.button(
                        "Apply", key=f"apply_status_{comment_id}"
                    ):
                        try:
                            update_comment_status(catalog, DEFAULT_METADATA_SCHEMA, comment_id, new_status)
                            st.success(f"Status updated to {new_status}.")
                            st.rerun()
                        except GovernanceError as exc:
                            st.error(str(exc))

                with col_reply:
                    try:
                        replies_df = load_replies(catalog, DEFAULT_METADATA_SCHEMA, comment_id)
                    except GovernanceError as exc:
                        st.error(str(exc))
                        replies_df = None

                    if replies_df is not None and not replies_df.empty:
                        for _, reply in replies_df.iterrows():
                            st.markdown(f"↳ *{reply['author']}* ({reply['created_at']}): {reply['reply_text']}")

                    with st.form(key=f"reply_form_{comment_id}", clear_on_submit=True):
                        reply_text = st.text_input("Add a reply", key=f"reply_text_{comment_id}")
                        if st.form_submit_button("Reply"):
                            if not reply_text.strip() or not reviewer_name.strip():
                                st.warning("Enter your name above and a reply first.")
                            else:
                                try:
                                    add_reply(
                                        catalog, DEFAULT_METADATA_SCHEMA, comment_id,
                                        reply_text.strip(), reviewer_name.strip(),
                                    )
                                    st.rerun()
                                except GovernanceError as exc:
                                    st.error(str(exc))
