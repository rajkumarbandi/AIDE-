"""The single configuration object: catalog, schema, and every connection
secret (workspace host, HTTP path — which encodes the SQL warehouse —
token, Gemini key), in one place.

This is a thin, unifying facade over two mechanisms that were each already
individually single-sourced (audited directly, not assumed):
  - catalog/schema come from components.filters.get_filters(), the one
    session-state-backed value every page already reads — changing the
    sidebar "Catalog" field updates it for every page and every AI query
    without a code change, with no separate per-module resolution anywhere.
  - connection secrets come from utils.secrets.get_setting(), the one
    env-var-then-st.secrets resolver already shared identically by
    utils.databricks and utils.gemini.
No module computes any of these seven values independently; every module
that needs one gets it from get_app_config() (or, for catalog/schema
specifically inside a page, get_filters() directly — the same single
source get_app_config() itself reads).
"""

from dataclasses import dataclass
from typing import Optional

from components.filters import get_filters
from utils.secrets import get_setting


@dataclass(frozen=True)
class AppConfig:
    catalog: str
    schema: str
    server_hostname: Optional[str]
    http_path: Optional[str]  # encodes which SQL warehouse this app talks to
    token: Optional[str]
    gemini_api_key: Optional[str]


def get_app_config() -> AppConfig:
    """The one accessor for every configured value the app needs — catalog,
    schema, and every Databricks/Gemini connection secret.
    """
    filters = get_filters()
    return AppConfig(
        catalog=filters["catalog"],
        schema=filters["schema"],
        server_hostname=get_setting("DATABRICKS_SERVER_HOSTNAME", ("databricks", "server_hostname")),
        http_path=get_setting("DATABRICKS_HTTP_PATH", ("databricks", "http_path")),
        token=get_setting("DATABRICKS_TOKEN", ("databricks", "access_token")),
        gemini_api_key=get_setting("GEMINI_API_KEY", ("gemini", "api_key")),
    )
