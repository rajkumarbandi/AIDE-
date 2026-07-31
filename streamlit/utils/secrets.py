"""Layered configuration resolution: environment variables first, then
st.secrets, then a caller-supplied default.

Azure Databricks Apps' documented secrets mechanism is env-var injection
(app.yaml's `valueFrom`, bound to a bound resource) — there is no official
support for .streamlit/secrets.toml there. Locally, secrets.toml remains the
convenient path. Checking env vars first means the same code works
unmodified in both places; never raises, so a missing value degrades to
`default` (typically None) rather than crashing the app.
"""

import os
from typing import Optional, Sequence

import streamlit as st


def get_setting(env_var: str, secrets_path: Sequence[str], default: Optional[str] = None) -> Optional[str]:
    """Resolve a config value: env var -> st.secrets[secrets_path...] -> default.

    `secrets_path` is a sequence of nested keys, e.g. ("databricks", "server_hostname").
    A missing secrets.toml (expected in most Databricks Apps deployments, and
    during local development before one is created) is treated the same as
    "not found there" — not an error.
    """
    value = os.environ.get(env_var)
    if value:
        return value

    node = st.secrets
    try:
        for key in secrets_path:
            node = node[key]
        if node:
            return str(node)
    except Exception:
        pass

    return default
