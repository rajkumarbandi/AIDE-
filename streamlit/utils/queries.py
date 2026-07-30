"""SQL query templates and the warehouse relationship registry.

Query functions are plain functions returning SQL strings for
utils.databricks.run_query() to execute — parametrized by catalog/schema,
never hardcoded, and deliberately not an ORM/query-builder (no abstraction
beyond "build the string").
"""

from utils.config import DEFAULT_CATALOG, DEFAULT_GOLD_SCHEMA, DEFAULT_METADATA_SCHEMA


def sql_table_metadata(
    catalog: str = DEFAULT_CATALOG, metadata_schema: str = DEFAULT_METADATA_SCHEMA
) -> str:
    """Every row from the AI Metadata Collector's output (currently Bronze-scope —
    see KNOWN_SILVER_TABLES/KNOWN_GOLD_TABLES below for why the graph doesn't stop
    there).
    """
    return f"SELECT * FROM {catalog}.{metadata_schema}.table_metadata"


def sql_ai_analysis(
    catalog: str = DEFAULT_CATALOG,
    metadata_schema: str = DEFAULT_METADATA_SCHEMA,
    table_name: str = None,
) -> str:
    """AI-generated analysis rows, optionally filtered to one table."""
    base = f"SELECT * FROM {catalog}.{metadata_schema}.ai_analysis"
    if table_name:
        safe_name = table_name.replace("'", "")
        base += f" WHERE table_name = '{safe_name}'"
    return base + " ORDER BY analysis_timestamp DESC"


def sql_kpi_summary(catalog: str = DEFAULT_CATALOG, gold_schema: str = DEFAULT_GOLD_SCHEMA) -> str:
    """The one-row KPI snapshot written by 03_gold/07_sales_dashboard.py."""
    return f"SELECT * FROM {catalog}.{gold_schema}.sales_kpi_summary LIMIT 1"


def sql_information_schema_tables(catalog: str, schema: str) -> str:
    """Every table in a given catalog.schema, for the Warehouse Explorer's browser."""
    return (
        f"SELECT table_name, table_type FROM {catalog}.information_schema.tables "
        f"WHERE table_schema = '{schema}' ORDER BY table_name"
    )


def sql_preview_table(catalog: str, schema: str, table_name: str, limit: int = 50) -> str:
    """A bounded row preview for the Warehouse Explorer / SQL Playground."""
    return f"SELECT * FROM {catalog}.{schema}.{table_name} LIMIT {int(limit)}"


# ---------------------------------------------------------------------------
# Warehouse relationship registry
# ---------------------------------------------------------------------------
# Unity Catalog has no *enforced* FK constraints on these tables (none were
# declared when the Gold notebooks were built), so there is no live metadata
# query that returns them. This list is not invented — it is a direct
# transcription of the FK resolution actually implemented in
# 03_gold/06_fact_sales.py (build_fact_sales()); every edge here corresponds to
# a real .join()/.alias() in that notebook, not a guess. If fact_sales' join
# logic ever changes, this list must be updated to match.
GOLD_RELATIONSHIPS = [
    {
        "from_table": "fact_sales",
        "from_column": "customer_key",
        "to_table": "dim_customer",
        "to_column": "customer_key",
        "description": "Each sales order line is attributed to one customer.",
    },
    {
        "from_table": "fact_sales",
        "from_column": "product_key",
        "to_table": "dim_product",
        "to_column": "product_key",
        "description": "Each sales order line references exactly one product.",
    },
    {
        "from_table": "fact_sales",
        "from_column": "territory_key",
        "to_table": "dim_territory",
        "to_column": "territory_key",
        "description": "Each sale is attributed to one sales territory (or Unknown, key -1).",
    },
    {
        "from_table": "fact_sales",
        "from_column": "salesperson_key",
        "to_table": "dim_salesperson",
        "to_column": "salesperson_key",
        "description": "Each sale may be attributed to a salesperson (or Unknown, "
        "for online orders).",
    },
    {
        "from_table": "fact_sales",
        "from_column": "order_date_key",
        "to_table": "dim_date",
        "to_column": "date_key",
        "description": "Each sale occurred on one calendar date.",
    },
]

# Known Silver/Gold table names, from having built 02_silver/*.py and
# 03_gold/*.py directly — real, not invented, but not yet AI-analyzed, since
# 02_metadata_collector.py currently only scans the bronze schema (see
# docs/silver_gold_warehouse_design.md "Future Scope": extending AI Metadata
# Analysis to Silver/Gold is exactly this gap). Used so the graph shows the
# full Bronze -> Silver -> Gold shape even before that extension exists.
KNOWN_SILVER_TABLES = [
    "customer", "person", "address", "state_province", "country_region",
    "business_entity_address", "sales_territory", "product", "product_subcategory",
    "product_category", "sales_person", "employee", "sales_order_header",
    "sales_order_detail", "store", "currency",
]

KNOWN_GOLD_TABLES = [
    "dim_date", "dim_customer", "dim_product", "dim_territory", "dim_salesperson",
    "fact_sales",
]

# Bronze -> Silver lineage: exactly the read_bronze_table() source(s) each
# 02_silver/*.py notebook uses to build that Silver table. Bronze table names
# here are the *ugly* fallback-ingestion names (e.g. "stateprovince"), matching
# what those notebooks actually pass to read_bronze_table().
SILVER_LINEAGE = {
    "customer": ["customer"],
    "person": ["person"],
    "address": ["address"],
    "state_province": ["stateprovince"],
    "country_region": ["countryregion"],
    "business_entity_address": ["businessentityaddress"],
    "sales_territory": ["salesterritory"],
    "product": ["product"],
    "product_subcategory": ["productsubcategory"],
    "product_category": ["productcategory"],
    "sales_order_header": ["sales_order_header"],
    "sales_order_detail": ["sales_order_detail"],
    "sales_person": ["salesperson"],
    "employee": ["employee"],
    "store": ["store"],
    "currency": ["currency"],
}

# Silver -> Gold lineage: exactly the read_silver_table() sources each
# 03_gold/*.py notebook's build_*() function uses to construct that table
# (fact_sales' additional Silver reads for FK/revenue *validation* are
# excluded here — this is build lineage, not every table touched).
GOLD_LINEAGE = {
    "dim_date": [],
    "dim_customer": [
        "customer", "person", "store", "business_entity_address", "address",
        "state_province", "country_region", "sales_territory", "sales_order_header",
    ],
    "dim_product": ["product", "product_subcategory", "product_category"],
    "dim_territory": ["sales_territory", "country_region"],
    "dim_salesperson": ["sales_person", "employee", "person", "sales_territory"],
    "fact_sales": ["sales_order_header", "sales_order_detail"],
}
