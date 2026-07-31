"""Current-user identity resolution for automatic reviewer attribution.

On Azure Databricks Apps, the platform's reverse proxy injects HTTP headers
identifying the authenticated workspace user into every request — verified
directly against Databricks' documentation ("Access HTTP headers passed to
Databricks apps": X-Forwarded-Email, X-Forwarded-Preferred-Username,
X-Forwarded-User) and Streamlit's st.context.headers API (introduced in
Streamlit 1.37.0; confirmed present in this app's pinned 1.60.0).

Running this app outside Databricks Apps (local development), none of those
headers exist at all — Databricks' own docs state there is no local
equivalent — so this falls back to `SELECT current_user()` over the SQL
connection (the identity of whichever credential is configured), and
finally to a clearly-labeled "Unknown" if even that fails. Never raises.

Deliberately NOT using On-Behalf-Of (OBO) tokens: OBO is Public Preview,
requires a workspace admin to enable it and specific OAuth scopes configured
on the app, and exists to let queries run *as* the user (for Unity Catalog
row/column security) — overkill for "attribute this comment to a name,"
which the identity headers alone already solve without any admin action.
"""

import logging
from typing import Optional, TypedDict

import streamlit as st

from utils.databricks import DatabricksConnectionError, DatabricksQueryError, run_query

logger = logging.getLogger("aide.identity")


class CurrentUser(TypedDict):
    name: str
    email: Optional[str]
    source: str  # "databricks_apps" | "sql_current_user" | "unavailable"


def get_current_user() -> CurrentUser:
    """Resolve the identity to attribute a governance action to. Never raises."""
    try:
        headers = st.context.headers
        email = headers.get("x-forwarded-email")
        preferred_username = headers.get("x-forwarded-preferred-username")
        forwarded_user = headers.get("x-forwarded-user")
        if email or preferred_username or forwarded_user:
            return {
                "name": preferred_username or email or forwarded_user,
                "email": email,
                "source": "databricks_apps",
            }
    except Exception as exc:
        logger.warning("Could not read Databricks Apps identity headers: %s", exc)

    try:
        df = run_query("SELECT current_user() AS user")
        if not df.empty:
            identity = df.iloc[0]["user"]
            if identity:
                return {
                    "name": identity,
                    "email": identity if "@" in identity else None,
                    "source": "sql_current_user",
                }
    except (DatabricksConnectionError, DatabricksQueryError) as exc:
        logger.warning("Could not resolve current_user() from the SQL connection: %s", exc)

    return {"name": "Unknown", "email": None, "source": "unavailable"}
