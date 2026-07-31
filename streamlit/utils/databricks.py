"""Databricks SQL Warehouse connectivity.

This app runs outside Databricks (a standalone Streamlit app), so it connects
via the databricks-sql-connector DB-API client, not dbutils — credentials come
from st.secrets, never hardcoded.
"""

import logging
import time
from typing import Optional, Tuple

import pandas as pd
import sqlparse
import streamlit as st
from databricks import sql as databricks_sql
from sqlparse.sql import Identifier, IdentifierList, Parenthesis
from sqlparse.tokens import CTE, Keyword

from utils.config import DATA_CACHE_TTL_SECONDS
from utils.secrets import get_setting

logger = logging.getLogger("aide.databricks")

# databricks-sql-connector's _socket_timeout defaults to None (no timeout) for
# socket send/recv/connect — verified via Connection.__init__'s internal-kwargs
# docstring. Without this, an unreachable host or a stalled warehouse hangs the
# connection indefinitely, and since get_connection() is @st.cache_resource,
# that hang blocks every user sharing this cached connection, not just one page.
_CONNECTION_TIMEOUT_SECONDS = 30

# Verified against sqlparse 0.5.3: get_type() classifies the statement's DML
# kind (SELECT/INSERT/UPDATE/...), and flatten() walks every leaf token
# (including inside subqueries/CTEs) so a forbidden keyword can't hide in a
# nested clause. Exact-value comparison (not substring) means a comment or
# identifier merely containing e.g. "drop" never false-positives.
_FORBIDDEN_KEYWORDS = frozenset(
    {
        "DROP", "DELETE", "TRUNCATE", "UPDATE", "INSERT", "ALTER", "CREATE",
        "GRANT", "REVOKE", "MERGE", "EXEC", "EXECUTE", "REPLACE",
    }
)


class DatabricksConnectionError(Exception):
    """Raised when the Databricks SQL Warehouse connection cannot be established."""


class DatabricksQueryError(Exception):
    """Raised when a query fails to execute."""


class UnsafeQueryError(Exception):
    """Raised when a query isn't a single, plain SELECT statement — SQL
    Playground and the AI Assistant are read-only surfaces.
    """


def validate_select_only(sql: str) -> None:
    """Raise UnsafeQueryError unless `sql` is exactly one SELECT statement with
    no forbidden DDL/DML keyword anywhere in it (including nested subqueries).
    """
    statements = [s for s in sqlparse.parse(sql) if s.tokens]
    if not statements:
        raise UnsafeQueryError("Empty query.")
    if len(statements) > 1:
        raise UnsafeQueryError("Only a single SELECT statement is allowed (no stacked queries).")

    statement = statements[0]
    if statement.get_type() != "SELECT":
        raise UnsafeQueryError(
            f"Only SELECT queries are allowed here (detected: {statement.get_type()})."
        )

    tokens = {t.value.upper() for t in statement.flatten() if t.ttype is not None}
    forbidden_hit = tokens & _FORBIDDEN_KEYWORDS
    if forbidden_hit:
        raise UnsafeQueryError(f"Query contains a forbidden keyword: {', '.join(forbidden_hit)}.")


def _identifier_ref_name(identifier: Identifier) -> Optional[str]:
    """Reconstruct the dotted name portion of a FROM/JOIN target, excluding
    its alias — e.g. Identifier("aide.gold.fact_sales f") -> "aide.gold.fact_sales".

    Identifier.get_real_name()/get_parent_name() only handle a single dot
    correctly (verified directly: for a 3-part name they return the MIDDLE
    and FIRST segments, not the table name) — not usable for catalog.schema.table.
    Returns None for anything that isn't a simple dotted name (a subquery or
    a bare function call), so the caller can skip it rather than misreport it.
    """
    name_parts = []
    for tok in identifier.tokens:
        if tok.is_whitespace:
            break
        if isinstance(tok, Parenthesis):
            return None
        name_parts.append(str(tok))
    name = "".join(name_parts).strip()
    return name or None


def _walk_table_refs(tokens, refs: list, cte_names: set) -> None:
    """Recursively collect every FROM/JOIN table reference (as raw dotted-name
    strings) and every CTE name declared by a WITH clause, walking into
    subqueries/CTE bodies (Parenthesis groups) too — a wrong catalog inside a
    nested subquery is just as real a bug as one at the top level.
    """
    expecting = None  # None | "table" | "cte"
    for tok in tokens:
        if tok.is_whitespace:
            continue

        if expecting == "cte":
            if isinstance(tok, Identifier):
                cte_names.add(tok.get_real_name())
                for sub in tok.tokens:
                    if isinstance(sub, Parenthesis):
                        _walk_table_refs(sub.tokens, refs, cte_names)
            elif isinstance(tok, IdentifierList):
                for item in tok.get_identifiers():
                    cte_names.add(item.get_real_name())
                    for sub in item.tokens:
                        if isinstance(sub, Parenthesis):
                            _walk_table_refs(sub.tokens, refs, cte_names)
            expecting = None
            continue

        if expecting == "table":
            if isinstance(tok, Identifier):
                name = _identifier_ref_name(tok)
                if name:
                    refs.append(name)
                for sub in tok.tokens:
                    if isinstance(sub, Parenthesis):
                        _walk_table_refs(sub.tokens, refs, cte_names)
            elif isinstance(tok, IdentifierList):
                for item in tok.get_identifiers():
                    name = _identifier_ref_name(item)
                    if name:
                        refs.append(name)
            elif isinstance(tok, Parenthesis):
                _walk_table_refs(tok.tokens, refs, cte_names)
            expecting = None
            continue

        if tok.ttype is CTE:
            expecting = "cte"
        elif tok.ttype is Keyword and tok.value.upper() == "FROM":
            expecting = "table"
        elif tok.ttype is Keyword and "JOIN" in tok.value.upper():
            expecting = "table"
        elif isinstance(tok, Parenthesis):
            _walk_table_refs(tok.tokens, refs, cte_names)


def validate_catalog_only(sql: str, expected_catalog: str) -> None:
    """Raise UnsafeQueryError unless every FROM/JOIN table reference in `sql`
    is fully qualified with exactly `expected_catalog` — a defensive backstop
    for the AI Assistant, since a real production bug had Gemini invent a
    plausible-looking but wrong catalog name (e.g. "rg_dev_sandbox") when its
    prompt context didn't spell the catalog out explicitly enough. CTE names
    (from a WITH clause) are excluded, since they're not real tables and
    never carry a catalog prefix.
    """
    for statement in sqlparse.parse(sql):
        if not statement.tokens:
            continue
        refs: list = []
        cte_names: set = set()
        _walk_table_refs(statement.tokens, refs, cte_names)

        for ref in refs:
            parts = ref.split(".")
            bare_name = parts[-1]
            if bare_name in cte_names:
                continue
            if len(parts) < 3:
                raise UnsafeQueryError(
                    f"Table reference \"{ref}\" is not fully qualified with the catalog "
                    f"(expected {expected_catalog}.<schema>.<table>)."
                )
            if parts[0] != expected_catalog:
                raise UnsafeQueryError(
                    f"Table reference \"{ref}\" uses catalog \"{parts[0]}\" instead of the "
                    f"configured catalog \"{expected_catalog}\"."
                )


def _get_connection_params() -> dict:
    """Resolve connection params: environment variables (Databricks Apps'
    documented secrets mechanism) first, then st.secrets (local dev).
    """
    server_hostname = get_setting("DATABRICKS_SERVER_HOSTNAME", ("databricks", "server_hostname"))
    http_path = get_setting("DATABRICKS_HTTP_PATH", ("databricks", "http_path"))
    access_token = get_setting("DATABRICKS_TOKEN", ("databricks", "access_token"))

    if not (server_hostname and http_path and access_token):
        raise DatabricksConnectionError(
            "Databricks connection is not configured. Set DATABRICKS_SERVER_HOSTNAME, "
            "DATABRICKS_HTTP_PATH, and DATABRICKS_TOKEN as environment variables (Databricks "
            "Apps — see app.yaml) or add a [databricks] section (server_hostname, http_path, "
            "access_token) to .streamlit/secrets.toml (local development)."
        )
    return {
        "server_hostname": server_hostname,
        "http_path": http_path,
        "access_token": access_token,
    }


@st.cache_resource(show_spinner=False)
def get_connection():
    """Return a cached Databricks SQL connection, created once per app session."""
    params = _get_connection_params()
    try:
        return databricks_sql.connect(**params, _socket_timeout=_CONNECTION_TIMEOUT_SECONDS)
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


def run_query_timed(sql: str) -> Tuple[pd.DataFrame, float]:
    """run_query() plus wall-clock elapsed seconds, for the SQL Playground's
    "Execution Time" display. A near-zero time on a repeat query correctly
    reflects a cache hit, not a bug.
    """
    start = time.perf_counter()
    df = run_query(sql)
    return df, time.perf_counter() - start


def is_connected() -> bool:
    """Best-effort connectivity check for the sidebar status indicator."""
    try:
        get_connection()
        return True
    except Exception:
        return False
