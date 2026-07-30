"""Generic, table-agnostic formatting helpers reused across pages/components.

Plain functions only — no classes, no decorators. Anything table-specific or
page-specific belongs in that page, not here.
"""

from typing import Optional


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


def status_from_pct(value_pct: float, warn_threshold: float = 1.0) -> str:
    """Green at 0%, amber up to warn_threshold, red beyond — same convention as
    the notebooks' get_status_color (null%/duplicate% cards).
    """
    if value_pct == 0:
        return "success"
    if value_pct <= warn_threshold:
        return "warning"
    return "error"
