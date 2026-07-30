"""Generic, table-agnostic formatting helpers reused across pages/components.

Plain functions only — no classes, no decorators. Anything table-specific or
page-specific belongs in that page, not here.
"""

import json
from typing import List, Optional

import numpy as np


def format_currency(value: Optional[float]) -> str:
    """1234.5 -> '$1,234.50'. None/NaN-safe."""
    if value is None:
        return "—"
    try:
        return f"${float(value):,.2f}"
    except (TypeError, ValueError):
        return "—"


def format_number(value: Optional[float]) -> str:
    """1234 -> '1,234'. None/NaN-safe."""
    if value is None:
        return "—"
    try:
        return f"{float(value):,.0f}"
    except (TypeError, ValueError):
        return "—"


def format_pct(value: Optional[float], decimals: int = 1) -> str:
    """0.1234 -> '12.3%'. Assumes `value` is a fraction (0-1), not already *100."""
    if value is None:
        return "—"
    try:
        return f"{float(value) * 100:.{decimals}f}%"
    except (TypeError, ValueError):
        return "—"


def truncate_text(text: Optional[str], max_length: int = 120) -> str:
    """Truncate long text (e.g. an AI-generated description) for compact display."""
    if not text:
        return ""
    return text if len(text) <= max_length else text[: max_length - 1].rstrip() + "…"


def parse_nested_field(value) -> List[dict]:
    """Normalize a nested ARRAY<STRUCT> column (e.g. table_metadata.columns) to
    a plain list of dicts.

    Verified against the installed databricks-sql-connector: with its default
    `_use_arrow_native_complex_types=True`, ARRAY comes back as a numpy.ndarray
    and STRUCT as a dict — not a plain Python list, and not a JSON string. Both
    of those legacy shapes are handled too (as a fallback for
    `_use_arrow_native_complex_types=False` or a future connector change),
    so an unexpected shape degrades to an empty list instead of crashing.
    """
    if value is None:
        return []
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except (TypeError, ValueError):
            return []
        return parsed if isinstance(parsed, list) else []
    if isinstance(value, (list, tuple, np.ndarray)):
        result = []
        for item in value:
            if isinstance(item, dict):
                result.append(item)
            else:
                try:
                    result.append(dict(item))
                except (TypeError, ValueError):
                    continue
        return result
    return []


def to_plain_list(value) -> list:
    """Normalize an ARRAY<primitive> column (e.g. table_metadata.
    primary_key_candidates) to a plain Python list.

    Same root cause as parse_nested_field (databricks-sql-connector returns
    ARRAY as numpy.ndarray by default) but for primitive elements, not
    structs — `dict(item)` would wrongly drop every string element, so this
    is a separate helper rather than reusing parse_nested_field. A bare
    numpy.ndarray must never reach an `if value:` truth-value check — that
    raises ValueError for arrays with more than one element.
    """
    if value is None:
        return []
    if isinstance(value, np.ndarray):
        return value.tolist()
    if isinstance(value, (list, tuple)):
        return list(value)
    return [value]


def status_from_pct(value_pct: float, warn_threshold: float = 1.0) -> str:
    """Green at 0%, amber up to warn_threshold, red beyond — same convention as
    the notebooks' get_status_color (null%/duplicate% cards).
    """
    if value_pct == 0:
        return "success"
    if value_pct <= warn_threshold:
        return "warning"
    return "error"
