"""Databricks SQL Warehouse connectivity.

This app runs outside Databricks (a standalone Streamlit app), so it connects
via the databricks-sql-connector DB-API client, not dbutils — credentials come
from st.secrets, never hardcoded.
"""

import logging

import pandas as pd
import streamlit as st
from databricks import sql as databricks_sql

from utils.config import DATA_CACHE_TTL_SECONDS

logger = logging.getLogger("aide.databricks")


class DatabricksConnectionError(Exception):
    """Raised when the Databricks SQL Warehouse connection cannot be established."""


class DatabricksQueryError(Exception):
    """Raised when a query fails to execute."""


def _get_connection_params() -> dict:
    try:
        creds = st.secrets["databricks"]
        return {
            "server_hostname": creds["server_hostname"],
            "http_path": creds["http_path"],
            "access_token": creds["access_token"],
        }
    except Exception as exc:
        raise DatabricksConnectionError(
            "Databricks connection is not configured. Add a [databricks] section "
            "(server_hostname, http_path, access_token) to .streamlit/secrets.toml."
        ) from exc


@st.cache_resource(show_spinner=False)
def get_connection():
    """Return a cached Databricks SQL connection, created once per app session."""
    params = _get_connection_params()
    try:
        return databricks_sql.connect(**params)
    except Exception as exc:
        logger.error("Failed to connect to Databricks: %s", exc)
        raise DatabricksConnectionError(f"Could not connect to Databricks: {exc}") from exc


@st.cache_data(ttl=DATA_CACHE_TTL_SECONDS, show_spinner=False)
def run_query(sql: str) -> pd.DataFrame:
    """Execute a SQL query and return the results as a DataFrame.

    Cached for DATA_CACHE_TTL_SECONDS so repeated page renders (e.g. switching
    global filters) don't re-run identical queries against the warehouse.
    """
    connection = get_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute(sql)
            columns = [col[0] for col in cursor.description]
            rows = cursor.fetchall()
        return pd.DataFrame(rows, columns=columns)
    except DatabricksConnectionError:
        raise
    except Exception as exc:
        logger.error("Query failed: %s | sql=%s", exc, sql)
        raise DatabricksQueryError(f"Query failed: {exc}") from exc


def is_connected() -> bool:
    """Best-effort connectivity check for the sidebar status indicator."""
    try:
        get_connection()
        return True
    except Exception:
        return False
