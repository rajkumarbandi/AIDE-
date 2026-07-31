"""Data governance: reviewer comments on tables/columns, with a status
workflow (New → Flagged/Under Review → Approved/Rejected/Resolved/
Implemented) and a reply thread per comment.

This is the app's first WRITE path — every other page is strictly read-only
(validate_select_only is enforced everywhere else in the app). Every
user-submitted VALUE here goes through the Databricks SQL connector's native
parameter binding (PEP-249 "named" paramstyle: `:param_name` placeholders in
the SQL text plus a dict of values passed to cursor.execute()) — verified
directly against the installed connector's Cursor.execute() docstring, never
raw string interpolation of user text into SQL. Table/schema names are still
f-string-interpolated, same as every read query elsewhere in this app: those
come from our own config, not raw user input.

Requires the connected Databricks credential to have CREATE TABLE/INSERT/
UPDATE grants on the metadata schema — a new requirement, since every other
feature in this app only ever needed SELECT. A missing grant surfaces as a
clear, caught GovernanceError rather than crashing the app.
"""

import logging
import uuid
from typing import Optional

import pandas as pd
import streamlit as st

from utils.databricks import DatabricksConnectionError, get_connection

logger = logging.getLogger("aide.governance")

STATUSES = ["New", "Flagged", "Under Review", "Approved", "Rejected", "Resolved", "Implemented"]
OPEN_STATUSES = ["New", "Flagged", "Under Review"]
PRIORITIES = ["Low", "Medium", "High", "Critical"]

_COMMENTS_TABLE = "governance_comments"
_REPLIES_TABLE = "governance_comment_replies"


class GovernanceError(Exception):
    """Raised when a governance read/write fails — e.g. missing CREATE
    TABLE/INSERT/UPDATE grants on the metadata schema.
    """


def _full_table(catalog: str, schema: str, table: str) -> str:
    return f"{catalog}.{schema}.{table}"


def _execute(sql: str, params: Optional[dict] = None) -> None:
    try:
        connection = get_connection()
        with connection.cursor() as cursor:
            cursor.execute(sql, params)
    except DatabricksConnectionError:
        raise
    except Exception as exc:
        logger.error("Governance write failed: %s | sql=%s", exc, sql)
        raise GovernanceError(
            f"Could not complete that action — this may mean the connected Databricks "
            f"credential lacks write permission on the metadata schema. ({exc})"
        ) from exc


def _fetch(sql: str) -> pd.DataFrame:
    """A dedicated, UNCACHED read path (unlike utils.databricks.run_query) —
    governance data must reflect a write immediately (e.g. right after
    submitting a comment), so the shared, TTL-cached run_query() would show
    stale results here.
    """
    try:
        connection = get_connection()
        with connection.cursor() as cursor:
            cursor.execute(sql)
            columns = [col[0] for col in cursor.description]
            rows = cursor.fetchall()
        return pd.DataFrame(rows, columns=columns)
    except DatabricksConnectionError:
        raise
    except Exception as exc:
        logger.error("Governance read failed: %s | sql=%s", exc, sql)
        raise GovernanceError(f"Could not load governance data: {exc}") from exc


@st.cache_resource(show_spinner=False)
def ensure_tables(catalog: str, schema: str) -> bool:
    """Create the governance tables if they don't exist yet — cached so this
    DDL runs at most once per app session, not on every page view.
    """
    comments_sql = f"""
    CREATE TABLE IF NOT EXISTS {_full_table(catalog, schema, _COMMENTS_TABLE)} (
        comment_id STRING,
        affected_table STRING,
        affected_column STRING,
        comment_text STRING,
        reason STRING,
        suggested_change STRING,
        author STRING,
        priority STRING,
        status STRING,
        created_at TIMESTAMP,
        updated_at TIMESTAMP
    ) USING DELTA
    """
    replies_sql = f"""
    CREATE TABLE IF NOT EXISTS {_full_table(catalog, schema, _REPLIES_TABLE)} (
        reply_id STRING,
        comment_id STRING,
        reply_text STRING,
        author STRING,
        created_at TIMESTAMP
    ) USING DELTA
    """
    _execute(comments_sql)
    _execute(replies_sql)
    return True


def submit_comment(
    catalog: str,
    schema: str,
    affected_table: str,
    comment_text: str,
    author: str,
    priority: str = "Medium",
    affected_column: Optional[str] = None,
    reason: Optional[str] = None,
    suggested_change: Optional[str] = None,
) -> str:
    """Insert a new reviewer comment with status "New"; returns its comment_id."""
    ensure_tables(catalog, schema)
    comment_id = str(uuid.uuid4())
    sql = f"""
    INSERT INTO {_full_table(catalog, schema, _COMMENTS_TABLE)}
    (comment_id, affected_table, affected_column, comment_text, reason, suggested_change,
     author, priority, status, created_at, updated_at)
    VALUES (:comment_id, :affected_table, :affected_column, :comment_text, :reason,
            :suggested_change, :author, :priority, 'New', current_timestamp(), current_timestamp())
    """
    _execute(
        sql,
        {
            "comment_id": comment_id,
            "affected_table": affected_table,
            "affected_column": affected_column,
            "comment_text": comment_text,
            "reason": reason,
            "suggested_change": suggested_change,
            "author": author,
            "priority": priority,
        },
    )
    return comment_id


def update_comment_status(catalog: str, schema: str, comment_id: str, new_status: str) -> None:
    if new_status not in STATUSES:
        raise ValueError(f"Unknown status: {new_status}")
    sql = f"""
    UPDATE {_full_table(catalog, schema, _COMMENTS_TABLE)}
    SET status = :new_status, updated_at = current_timestamp()
    WHERE comment_id = :comment_id
    """
    _execute(sql, {"new_status": new_status, "comment_id": comment_id})


def add_reply(catalog: str, schema: str, comment_id: str, reply_text: str, author: str) -> None:
    ensure_tables(catalog, schema)
    sql = f"""
    INSERT INTO {_full_table(catalog, schema, _REPLIES_TABLE)}
    (reply_id, comment_id, reply_text, author, created_at)
    VALUES (:reply_id, :comment_id, :reply_text, :author, current_timestamp())
    """
    _execute(
        sql,
        {
            "reply_id": str(uuid.uuid4()),
            "comment_id": comment_id,
            "reply_text": reply_text,
            "author": author,
        },
    )


def load_comments(catalog: str, schema: str, affected_table: Optional[str] = None) -> pd.DataFrame:
    """All comments, optionally filtered to one table — `affected_table`
    comes from our own already-validated NAV_PAGES/catalog data, not raw
    user text, so it's safe to interpolate here the same way every other
    read query in this app interpolates real table names.
    """
    ensure_tables(catalog, schema)
    sql = f"SELECT * FROM {_full_table(catalog, schema, _COMMENTS_TABLE)}"
    if affected_table:
        safe_name = affected_table.replace("'", "")
        sql += f" WHERE affected_table = '{safe_name}'"
    sql += " ORDER BY created_at DESC"
    return _fetch(sql)


def load_replies(catalog: str, schema: str, comment_id: str) -> pd.DataFrame:
    ensure_tables(catalog, schema)
    safe_id = comment_id.replace("'", "")
    sql = (
        f"SELECT * FROM {_full_table(catalog, schema, _REPLIES_TABLE)} "
        f"WHERE comment_id = '{safe_id}' ORDER BY created_at ASC"
    )
    return _fetch(sql)
