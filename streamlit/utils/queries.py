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
    """One row per (catalog_name, schema_name, table_name) from the AI Metadata
    Collector's output — 02_metadata_collector.py now covers all three medallion
    layers (Bronze, Silver, and Gold; previously Bronze-only), each written with
    a per-layer `replaceWhere` overwrite that should already guarantee at most
    one row per table. This stays defensive with a QUALIFY dedup anyway (a real,
    reported bug: the AI Data Catalog showed duplicate rows for the same table
    because the caller assumed this was still Bronze-only and separately unioned
    in a live information_schema listing for Silver/Gold, double-counting any
    table that already had AI-collected metadata — see
    pages/02_AI_Data_Catalog.py::_load_unified_catalog) — so a stale/legacy
    duplicate row can never resurface: the most recently collected row per table
    always wins.
    """
    return (
        f"SELECT * FROM {catalog}.{metadata_schema}.table_metadata "
        "QUALIFY ROW_NUMBER() OVER ("
        "PARTITION BY catalog_name, schema_name, table_name "
        "ORDER BY metadata_collected_at DESC"
        ") = 1"
    )


def sql_ai_analysis(
    catalog: str = DEFAULT_CATALOG,
    metadata_schema: str = DEFAULT_METADATA_SCHEMA,
    table_name: str = None,
) -> str:
    """The current AI analysis row per table, optionally filtered to one table.

    03_metadata_analyzer.py upserts (UPDATEs) exactly one row per (catalog,
    schema, table), so this should never see duplicates in practice — but stays
    defensive with a QUALIFY dedup: among any rows sharing the same key, a
    SUCCESS row always wins over PENDING/PROCESSING/FAILED, and ties break by
    the most recent analysis_timestamp. "Always use the latest successful
    analysis," not just the most recent attempt regardless of outcome.
    """
    base = f"SELECT * FROM {catalog}.{metadata_schema}.ai_analysis"
    if table_name:
        safe_name = table_name.replace("'", "")
        base += f" WHERE table_name = '{safe_name}'"
    return (
        base + " QUALIFY ROW_NUMBER() OVER ("
        "PARTITION BY catalog_name, schema_name, table_name "
        "ORDER BY CASE WHEN processing_status = 'SUCCESS' THEN 0 ELSE 1 END, "
        "analysis_timestamp DESC"
        ") = 1 ORDER BY analysis_timestamp DESC"
    )


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


def sql_information_schema_columns(catalog: str, schema: str, table_name: str) -> str:
    """Column name/type/nullability for one table — real Unity Catalog metadata,
    used by the AI Data Catalog's column detail view and Warehouse Explorer's
    profiling.
    """
    return (
        "SELECT column_name, data_type, is_nullable, ordinal_position "
        f"FROM {catalog}.information_schema.columns "
        f"WHERE table_schema = '{schema}' AND table_name = '{table_name}' "
        "ORDER BY ordinal_position"
    )


def sql_column_profile(catalog: str, schema: str, table_name: str, columns: list) -> str:
    """One-pass null-count + approximate-distinct-count profile for every column.

    Uses approx_count_distinct (not the notebooks' exact collect_set approach) —
    a deliberate difference: this runs on-demand against an arbitrary,
    potentially large table from an interactive UI, where responsiveness matters
    more than exactness (unlike the warehouse build, which profiles known,
    bounded AdventureWorks tables once per batch run).
    """
    metrics = ["COUNT(*) AS total_rows"]
    for column in columns:
        metrics.append(f"SUM(CASE WHEN `{column}` IS NULL THEN 1 ELSE 0 END) AS `{column}__nulls`")
        metrics.append(f"approx_count_distinct(`{column}`) AS `{column}__distinct`")
    return f"SELECT {', '.join(metrics)} FROM {catalog}.{schema}.{table_name}"


def sql_schema_columns(catalog: str, schema: str) -> str:
    """Every table.column in one schema, in one query — used to build the AI
    Assistant's real, live schema-grounding context for NL-to-SQL generation
    (never a static/guessed table list).
    """
    return (
        "SELECT table_name, column_name, data_type "
        f"FROM {catalog}.information_schema.columns "
        f"WHERE table_schema = '{schema}' ORDER BY table_name, ordinal_position"
    )


def sql_schema_table_column_counts(catalog: str, schema: str) -> str:
    """Column count per table in one schema, in ONE query — used to enrich
    the Silver/Gold catalog listing without a per-table round trip.
    """
    return (
        "SELECT table_name, COUNT(*) AS column_count "
        f"FROM {catalog}.information_schema.columns WHERE table_schema = '{schema}' "
        "GROUP BY table_name"
    )


def sql_row_count(catalog: str, schema: str, table_name: str) -> str:
    """Exact row count for one table — deliberately on-demand (called only
    for the currently-selected table in a detail view), never eagerly for
    every row of a catalog listing.
    """
    return f"SELECT COUNT(*) AS row_count FROM {catalog}.{schema}.{table_name}"


def sql_column_sample_values(catalog: str, schema: str, table_name: str, column: str, limit: int = 10) -> str:
    """Distinct, non-null sample values for one column — the Warehouse
    Explorer's column-profiling "sample values" view.
    """
    return (
        f"SELECT DISTINCT `{column}` AS sample_value "
        f"FROM {catalog}.{schema}.{table_name} "
        f"WHERE `{column}` IS NOT NULL LIMIT {int(limit)}"
    )


def sql_column_distribution(catalog: str, schema: str, table_name: str, column: str, limit: int = 15) -> str:
    """Real value-frequency distribution for one column (top N by count) — the
    Warehouse Explorer's column-profiling "distribution" chart.
    """
    return (
        f"SELECT `{column}` AS value, COUNT(*) AS frequency "
        f"FROM {catalog}.{schema}.{table_name} "
        f"GROUP BY `{column}` ORDER BY frequency DESC LIMIT {int(limit)}"
    )


# ---------------------------------------------------------------------------
# Executive Dashboard — fact_sales joined to every dimension, filterable by
# year / territory / product category / salesperson.
# ---------------------------------------------------------------------------


def _fact_joins(catalog: str, gold_schema: str) -> str:
    """The common FROM+JOIN block every dashboard query below builds on — kept
    in one place so the five joins aren't repeated in every query function.
    LEFT JOIN throughout: fact_sales' territory_key/salesperson_key resolve to
    the "Unknown" (-1) dimension member for unassigned orders, but LEFT JOIN
    degrades gracefully (a null territory/salesperson name, not a silently
    dropped row) even if that Unknown member were ever missing.
    """
    g = f"{catalog}.{gold_schema}"
    return (
        f"FROM {g}.fact_sales f "
        f"LEFT JOIN {g}.dim_date dd ON f.order_date_key = dd.date_key "
        f"LEFT JOIN {g}.dim_customer dc ON f.customer_key = dc.customer_key "
        f"LEFT JOIN {g}.dim_product dp ON f.product_key = dp.product_key "
        f"LEFT JOIN {g}.dim_territory dt ON f.territory_key = dt.territory_key "
        f"LEFT JOIN {g}.dim_salesperson ds ON f.salesperson_key = ds.salesperson_key"
    )


def _filter_conditions(filters: dict) -> list:
    """Translate the global filter dict into SQL WHERE fragments — only for
    dimensions with a real (non-"All") selection.
    """
    conditions = []
    year = filters.get("year")
    if year and year != "All":
        conditions.append(f"dd.year = {int(year)}")
    territory = filters.get("territory")
    if territory and territory != "All":
        conditions.append(f"dt.territory_name = '{territory.replace(chr(39), '')}'")
    category = filters.get("product_category")
    if category and category != "All":
        conditions.append(f"dp.category_name = '{category.replace(chr(39), '')}'")
    salesperson = filters.get("salesperson")
    if salesperson and salesperson != "All":
        conditions.append(f"ds.salesperson_name = '{salesperson.replace(chr(39), '')}'")
    return conditions


def _where_clause(conditions: list) -> str:
    return f"WHERE {' AND '.join(conditions)}" if conditions else ""


def sql_kpi_summary_live(catalog: str, gold_schema: str, filters: dict) -> str:
    """Live, filter-aware headline KPIs (total revenue/orders/customers/avg
    order value) computed directly from fact_sales + dimensions, using the
    same _fact_joins/_filter_conditions/_where_clause helpers every other
    dashboard query already uses — the Executive Dashboard's KPI row
    previously read the precomputed, all-time sales_kpi_summary table
    instead (see sql_kpi_summary below), which has no Year/Territory/
    Category/Salesperson dimensionality at all and could never respond to
    the sidebar filters. Top Territory/Top Product aren't included here —
    they need their own GROUP BY, so the dashboard page derives them from
    sql_revenue_by_territory/sql_top_products instead of a second query.
    """
    where = _where_clause(_filter_conditions(filters))
    return (
        "SELECT SUM(f.line_total) AS total_revenue, "
        "COUNT(DISTINCT f.sales_order_id) AS total_orders, "
        "COUNT(DISTINCT f.customer_key) AS total_customers, "
        "SUM(f.line_total) / NULLIF(COUNT(DISTINCT f.sales_order_id), 0) AS avg_order_value "
        f"{_fact_joins(catalog, gold_schema)} {where}"
    )


def sql_monthly_revenue_trend(catalog: str, gold_schema: str, filters: dict) -> str:
    where = _where_clause(_filter_conditions(filters))
    return (
        "SELECT dd.year, dd.month, dd.month_name, SUM(f.line_total) AS revenue "
        f"{_fact_joins(catalog, gold_schema)} {where} "
        "GROUP BY dd.year, dd.month, dd.month_name ORDER BY dd.year, dd.month"
    )


def sql_quarterly_revenue(catalog: str, gold_schema: str, filters: dict) -> str:
    where = _where_clause(_filter_conditions(filters))
    return (
        "SELECT dd.year, dd.quarter, SUM(f.line_total) AS revenue "
        f"{_fact_joins(catalog, gold_schema)} {where} "
        "GROUP BY dd.year, dd.quarter ORDER BY dd.year, dd.quarter"
    )


def sql_revenue_by_territory(catalog: str, gold_schema: str, filters: dict) -> str:
    where = _where_clause(_filter_conditions(filters))
    return (
        "SELECT dt.territory_name, SUM(f.line_total) AS revenue, "
        "COUNT(DISTINCT f.sales_order_id) AS orders "
        f"{_fact_joins(catalog, gold_schema)} {where} "
        "GROUP BY dt.territory_name ORDER BY revenue DESC"
    )


def sql_revenue_by_category(catalog: str, gold_schema: str, filters: dict) -> str:
    where = _where_clause(_filter_conditions(filters))
    return (
        "SELECT dp.category_name, SUM(f.line_total) AS revenue "
        f"{_fact_joins(catalog, gold_schema)} {where} "
        "GROUP BY dp.category_name ORDER BY revenue DESC"
    )


def sql_top_products(catalog: str, gold_schema: str, filters: dict, top_n: int) -> str:
    where = _where_clause(_filter_conditions(filters))
    return (
        "SELECT dp.product_name, dp.category_name, SUM(f.line_total) AS revenue, "
        "SUM(f.order_qty) AS units_sold "
        f"{_fact_joins(catalog, gold_schema)} {where} "
        f"GROUP BY dp.product_name, dp.category_name ORDER BY revenue DESC LIMIT {int(top_n)}"
    )


def sql_top_customers(catalog: str, gold_schema: str, filters: dict, top_n: int) -> str:
    where = _where_clause(_filter_conditions(filters))
    return (
        "SELECT dc.customer_name, dc.customer_type, SUM(f.line_total) AS revenue, "
        "COUNT(DISTINCT f.sales_order_id) AS orders "
        f"{_fact_joins(catalog, gold_schema)} {where} "
        f"GROUP BY dc.customer_name, dc.customer_type ORDER BY revenue DESC LIMIT {int(top_n)}"
    )


def sql_order_trend(catalog: str, gold_schema: str, filters: dict) -> str:
    where = _where_clause(_filter_conditions(filters))
    return (
        "SELECT dd.year, dd.month, dd.month_name, COUNT(DISTINCT f.sales_order_id) AS orders "
        f"{_fact_joins(catalog, gold_schema)} {where} "
        "GROUP BY dd.year, dd.month, dd.month_name ORDER BY dd.year, dd.month"
    )


def sql_customer_growth(catalog: str, gold_schema: str, filters: dict) -> str:
    """New distinct customers per month (first order in that month) — a simple,
    real growth signal, not a fabricated metric.
    """
    where = _where_clause(_filter_conditions(filters))
    return (
        "WITH first_orders AS ("
        "SELECT dc.customer_key, MIN(dd.full_date) AS first_order_date "
        f"{_fact_joins(catalog, gold_schema)} {where} GROUP BY dc.customer_key) "
        "SELECT YEAR(first_order_date) AS year, MONTH(first_order_date) AS month, "
        "COUNT(*) AS new_customers "
        "FROM first_orders GROUP BY YEAR(first_order_date), MONTH(first_order_date) "
        "ORDER BY year, month"
    )


def sql_distinct_years(catalog: str, gold_schema: str) -> str:
    """Years that actually have sales — not dim_date's full generated range."""
    g = f"{catalog}.{gold_schema}"
    return (
        f"SELECT DISTINCT dd.year FROM {g}.fact_sales f "
        f"JOIN {g}.dim_date dd ON f.order_date_key = dd.date_key ORDER BY dd.year DESC"
    )


def sql_distinct_territories(catalog: str, gold_schema: str) -> str:
    return (
        f"SELECT territory_name FROM {catalog}.{gold_schema}.dim_territory "
        "WHERE territory_key != -1 ORDER BY territory_name"
    )


def sql_distinct_categories(catalog: str, gold_schema: str) -> str:
    return (
        f"SELECT DISTINCT category_name FROM {catalog}.{gold_schema}.dim_product "
        "WHERE category_name IS NOT NULL ORDER BY category_name"
    )


def sql_distinct_salespersons(catalog: str, gold_schema: str) -> str:
    return (
        f"SELECT salesperson_name FROM {catalog}.{gold_schema}.dim_salesperson "
        "WHERE salesperson_key != -1 ORDER BY salesperson_name"
    )


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

def gold_keys_for_table(table_name: str) -> tuple:
    """Real PK/FK info for a Gold table, derived from GOLD_RELATIONSHIPS — the
    only layer with an explicit, verified key registry (Unity Catalog has no
    *enforced* FK constraints here to read live; see GOLD_RELATIONSHIPS'
    docstring above). Returns (primary_keys: list, foreign_keys: list of
    "column → table.column" strings).
    """
    primary_keys, foreign_keys = set(), []
    for rel in GOLD_RELATIONSHIPS:
        if rel["to_table"] == table_name:
            primary_keys.add(rel["to_column"])
        if rel["from_table"] == table_name:
            foreign_keys.append(f"{rel['from_column']} → {rel['to_table']}.{rel['to_column']}")
    return sorted(primary_keys), foreign_keys


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
