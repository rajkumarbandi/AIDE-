"""App-wide configuration constants.

Centralized here so nothing else in the app hardcodes a catalog/schema name,
color, or page label independently — mirrors the same "one shared config"
principle used by 02_silver/00_common_utils.py and 03_gold/00_gold_utils.py
in the notebooks.
"""

APP_TITLE = "AIDE — AI Data Engineering Assistant"
APP_VERSION = "2.0.0"

# Warehouse defaults — same catalog/schema names the notebooks write to.
DEFAULT_CATALOG = "aide"
DEFAULT_BRONZE_SCHEMA = "bronze"
DEFAULT_SILVER_SCHEMA = "silver"
DEFAULT_GOLD_SCHEMA = "gold"
DEFAULT_METADATA_SCHEMA = "metadata"

# Sidebar navigation: single source of truth for page path/title/icon, used by
# app.py (st.navigation) and by each page's breadcrumb header.
NAV_PAGES = [
    {"path": "pages/01_Executive_Dashboard.py", "title": "Executive Dashboard", "icon": "🏠"},
    {"path": "pages/02_AI_Data_Catalog.py", "title": "AI Data Catalog", "icon": "📚"},
    {"path": "pages/03_AI_Data_Model_Explorer.py", "title": "AI Data Model Explorer", "icon": "🧠"},
    {"path": "pages/04_Warehouse_Explorer.py", "title": "Warehouse Explorer", "icon": "🗄"},
    {"path": "pages/05_AI_Assistant.py", "title": "AI Assistant", "icon": "🤖"},
    {"path": "pages/06_SQL_Playground.py", "title": "SQL Playground", "icon": "💻"},
    {"path": "pages/07_Settings.py", "title": "Settings", "icon": "⚙"},
    {"path": "pages/08_Data_Governance.py", "title": "Data Governance", "icon": "🛡"},
]

# Layer/kind colors — used consistently by the AI Data Model Explorer graph AND
# any chart that breaks data down by medallion layer, so the same color always
# means the same thing everywhere in the app.
LAYER_COLORS = {
    "bronze": "#b45309",
    "silver": "#94a3b8",
    "gold": "#eab308",
    "fact": "#2563eb",
    "dimension": "#16a34a",
    "metadata": "#7c3aed",
}

# Status colors, matching the green/amber/red convention already used in the
# notebooks' HTML dashboards.
STATUS_COLORS = {
    "success": "#16a34a",
    "warning": "#d97706",
    "error": "#dc2626",
    "neutral": "#6b7280",
}

DEFAULT_TOP_N = 10
DATA_CACHE_TTL_SECONDS = 300
