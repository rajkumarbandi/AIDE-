# AIDE Enterprise Data Warehouse — Silver & Gold Design

**Status:** Design for review. Nothing in this document has been implemented yet.
**Scope:** Bronze (complete, not modified) → **Silver** → **Gold** → Streamlit → Business Review.

---

## 0. Grounding: what Bronze actually gives us

This design is built against the **real, already-ingested Bronze schema** (`config/adventureworks_tables.yaml`, 67 tables), not generic AdventureWorks assumptions. Only the tables actually needed for the Gold star schema and stated KPIs get a Silver counterpart — Silver is scoped to what Gold needs, not "all 69 tables."

Bronze tables feeding this design (verified column names/types against the real registry):

| Domain | Bronze tables |
|---|---|
| Customer | `customer`, `person`, `store`, `businessentityaddress`, `address`, `stateprovince`, `countryregion` |
| Product | `product`, `productsubcategory`, `productcategory` |
| Sales org | `salesperson`, `employee`, `salesterritory` |
| Transactions | `sales_order_header`, `sales_order_detail` |

`businessentity` is intentionally **not** carried into Silver — it holds no attributes beyond the shared surrogate key that `person`/`store`/`salesperson`/`employee` already carry, so it would be a pointless pass-through table.

Two authoritative facts pulled directly from `instawdb.sql` (not assumed) that shape the business rules below:
- `Person.PersonType`: `SC`=Store Contact, `IN`=Individual (retail) customer, `SP`=Sales person, `EM`=Employee (non-sales), `VC`=Vendor contact, `GC`=General contact.
- `SalesOrderHeader.Status`: `1`=In process, `2`=Approved, `3`=Backordered, `4`=Rejected, `5`=Shipped, `6`=Cancelled (the CHECK constraint allows 0–8; anything outside 1–6 is undefined in the source and should decode to `'Unknown'`, not be assumed).

---

## 1. Silver Layer Design

### Global Silver conventions (apply to every table, stated once)

| Convention | Why |
|---|---|
| Drop `rowguid` | SQL Server replication artifact. Zero analytical value, pure storage cost. |
| Trim all string columns | Whitespace hygiene; prevents silent join/group-by mismatches downstream. |
| Blank→NULL normalization already done | Bronze's `apply_schema` already nullifies blank source fields before casting — Silver does **not** need to repeat this. |
| Promote `*_flag`/`is_*` INT(0/1) → `BOOLEAN` | Safe now (Bronze already produced clean typed ints — the raw-string cast risk that kept these as `int` at Bronze doesn't apply to a cast from an already-typed int). |
| Rename generic `name` columns | `product_name`, `category_name`, `subcategory_name`, `territory_name`, `country_region_name`, `state_province_name`, `store_name` — once tables get joined at Gold, a bare `name` column is ambiguous. |
| Dedupe on natural/composite PK, keep latest by `modified_date` | Bronze's full-overwrite load shouldn't produce duplicates, but Silver should never assume that blindly. |
| Add `silver_load_timestamp`, `source_bronze_table` audit columns | Same lineage discipline as Bronze's audit columns; feeds the future AI Data Quality/RCA phase. |
| Write mode: full **overwrite** (not MERGE) | AdventureWorks is a static, full-refresh source with no CDC feed — MERGE would be complexity with nothing to reconcile against. A real production source with change data capture should use `MERGE INTO` on the natural key instead; noted here as the production upgrade path. |

### Silver tables

**1. `silver_country_region`** ← `bronze.countryregion`
Rename `name`→`country_region_name`. Trivial reference table.

**2. `silver_state_province`** ← `bronze.stateprovince`
Rename `name`→`state_province_name`. Cast `is_only_state_province_flag`→boolean.

**3. `silver_address`** ← `bronze.address`
Drop `spatial_location` (WKB-hex geography blob, unusable without a geospatial library and no KPI needs it — future enhancement if geospatial analysis is ever requested). `address_line2` stays nullable (legitimately optional).

**4. `silver_business_entity_address`** ← `bronze.businessentityaddress`
A bridge table (business_entity_id ↔ address_id ↔ address_type_id) — kept as-is at Silver grain (many rows per entity possible: billing vs. shipping vs. main office). Gold resolves this many-to-one when building `dim_customer` (see §2).

**5. `silver_person`** ← `bronze.person`
- Decode `person_type` → `person_type_description` using the verified source mapping above (business rule, not guesswork).
- Derive `full_name` = clean concatenation of `first_name`/`middle_name`/`last_name`, handling `middle_name IS NULL` without double-spacing.
- Drop `additional_contact_info`, `demographics` — raw XML with no clean tabular structure; no current KPI needs them. **Future scope:** parsing `demographics` XML would enable customer segmentation (age band, occupation) — a natural extension of the AI Metadata Analysis phase.

**6. `silver_store`** ← `bronze.store`
Rename `name`→`store_name`. Drop `demographics` XML (same reasoning as person). `sales_person_id` stays nullable (a store may have no assigned rep yet).

**7. `silver_product_category`** ← `bronze.productcategory`, **8. `silver_product_subcategory`** ← `bronze.productsubcategory`
Rename `name` columns. Trivial reference tables.

**9. `silver_product`** ← `bronze.product`
- Rename `name`→`product_name`, `class`→`product_class`, `style`→`product_style`.
- Cast `make_flag`, `finished_goods_flag` → boolean.
- Derive `product_status`: `'Discontinued'` if `discontinued_date IS NOT NULL`, `'Inactive'` if `sell_end_date` is set and in the past, else `'Active'` — directly needed to filter "Product Performance" KPIs to currently-sellable products.
- Derive `margin_pct = (list_price - standard_cost) / list_price * 100`, **guarded against `list_price = 0`** (raw-material/component products that are never sold directly have `list_price = 0` — a real, verified data shape, not a hypothetical edge case).
- `product_subcategory_id` stays nullable (components/raw materials legitimately have none).

**10. `silver_sales_territory`** ← `bronze.salesterritory`
- Rename `name`→`territory_name`, `group`→`territory_group` (both for clarity and to avoid the friction of a reserved-word-ish column name).
- **Explicitly do not** treat `sales_ytd`/`sales_last_year`/`cost_ytd`/`cost_last_year` as authoritative — they're the *source system's* point-in-time snapshot from whenever AdventureWorks was last refreshed, not something this pipeline computed. Gold's territory revenue KPIs are computed fresh from `fact_sales`; carrying these columns forward as if they were current would silently produce two different "territory revenue" numbers. Kept in Silver for reference/audit only.

**11. `silver_employee`** ← `bronze.employee`
Cast `salaried_flag`, `current_flag` → boolean. `organization_node` (hierarchyid) kept as an opaque string — not usable for hierarchy traversal without further parsing; flagged as future scope if an org-chart feature is ever wanted. `gender`/`marital_status` are already clean single-char codes, left as-is.

**12. `silver_sales_person`** ← `bronze.salesperson`
Derive `quota_attainment_pct = sales_ytd / sales_quota * 100`, guarded against `sales_quota` being `NULL` or `0` (new hires legitimately have no quota yet).

**13. `silver_customer`** ← `bronze.customer`
- Derive `customer_type`: `'Individual'` if `person_id IS NOT NULL`, `'Store'` if `store_id IS NOT NULL`, else `'Unknown'`. This is the single most important Silver business rule in this design — it directly feeds the Gold "Customer Type" KPI, computed once here rather than re-derived in every downstream query.
- **Validate** (don't silently trust) the source's implicit rule that a customer is *either* a person *or* a store, never both/neither — log a data-quality count of violations rather than failing the batch.
- `territory_id` stays nullable — Gold resolves unresolvable FKs to an "Unknown Territory" member (§2).

**14. `silver_sales_order_header`** ← `bronze.sales_order_header`
- Cast `online_order_flag` → boolean.
- Decode `status` → `order_status_description` using the verified mapping above; anything outside 1–6 → `'Unknown'` (defensive — the CHECK constraint technically allows 0–8, so don't assume only the six documented values ever appear).
- Validate `due_date >= order_date` and (`ship_date IS NULL OR ship_date >= order_date`) — re-checking the source's own CHECK constraints as a Silver-layer data-quality gate, since Silver is exactly where drift should be caught.
- Drop `credit_card_approval_code` (low business value; not carrying payment-adjacent data further than necessary is good practice even on synthetic data). **Keep** `comment` (small free-text field, low cost to retain, potential future input to AI-driven analysis — ties back to this project's own stated purpose).
- FK sanity checks (log, don't hard-fail): `customer_id` should exist in `silver_customer`; `sales_person_id`/`territory_id`, when non-null, should exist in `silver_sales_person`/`silver_sales_territory`.

**15. `silver_sales_order_detail`** ← `bronze.sales_order_detail`
- **Recompute** `line_total_calculated = ROUND(unit_price * (1 - unit_price_discount) * order_qty, 4)` and compare against the source's `line_total`, flagging `line_total_variance` where they disagree — validating a source-computed column rather than blindly trusting it. This is a genuine data-quality safeguard, and directly reusable by the future AI Data Quality & Root Cause Analyzer phase.
- Dedupe on composite key (`sales_order_id`, `sales_order_detail_id`).

---

## 2. Gold Layer Design — Star Schema

**Design principle: a *pure* star, not a snowflake.** No dimension has a foreign key to another dimension. Any hierarchical context (product category, territory's country) is **denormalized as flat attribute columns** directly on the relevant dimension — this is why there is no separate `dim_product_category` or `dim_country`; that context lives flat on `dim_product` and `dim_territory` respectively, exactly as Kimball dimensional modeling recommends.

**Design principle: no dangling fact FKs.** Every dimension gets a manually-inserted **"Unknown" member row** (surrogate key `-1`) for any fact row whose FK doesn't resolve (e.g. an order with no assigned salesperson). Missing-but-valid business states (e.g. `ship_date IS NULL` because the order hasn't shipped yet) use a genuine `NULL` in the fact row, not a sentinel — those are different situations and shouldn't be modeled the same way.

**Design principle: dimensions hold descriptive attributes, not measures.** `silver_sales_territory`'s `sales_ytd` etc. are deliberately *not* carried into `dim_territory` — territory revenue is computed by aggregating `fact_sales`, always, so there is exactly one source of truth for "territory revenue."

**Design principle: Type-1 for this phase.** All dimensions overwrite (latest-state-only); no history of attribute changes is tracked. AdventureWorks' `modified_date` columns mean SCD Type 2 could be added later without a Silver redesign — noted as a documented future enhancement, not built now (matches "Future Scope").

**Design principle: every Gold table builds from Silver only — never from another Gold table.** This was a deliberate simplification (see §4): `dim_customer`'s activity attributes (`is_active_customer`, first/last order date) are computed from `silver_sales_order_header` directly, not from `fact_sales`. The result is that all six Gold objects can be built **in parallel**, with no build-order dependency between them.

Catalog placement: `aide.gold.*`, mirroring the existing `aide.bronze`/`aide.metadata` schema-per-layer convention.

### `dim_date`
| | |
|---|---|
| **Purpose** | Time-intelligence (trend, YTD, QoQ, YoY) for every date-based KPI. |
| **Grain** | One row per calendar date. |
| **Primary Key** | `date_key` (INT, `YYYYMMDD` — a "smart key": sortable, human-readable, and the one dimension where reusing it as both surrogate and business key is standard Kimball practice). |
| **Foreign Keys** | None — generated, not sourced from Silver. |
| **Source** | Generated in code, spanning a fixed wide range (e.g. 2005-01-01–2030-12-31) rather than `MIN`/`MAX` of order dates, so new future orders never require regenerating the dimension. |
| **Business Logic** | Standard calendar attributes (year/quarter/month/day/day-of-week/is_weekend) plus AdventureWorks' fiscal year (starts **July 1** — a documented AdventureWorks convention, not an assumption to leave unverified during implementation). |
| **Aggregations** | None — pure calendar lookup. |
| **Refresh Strategy** | Build once; never needs re-running unless the date range needs extending. |

### `dim_product`
| | |
|---|---|
| **Purpose** | Product attributes for "Product Performance" KPIs (top products, revenue by product, units sold). |
| **Grain** | One row per product. |
| **Primary Key** | `product_key` (= `product_id`; natural key reused as surrogate — see Type-1 note above). |
| **Foreign Keys** | None (category/subcategory denormalized flat). |
| **Source Silver** | `silver_product`, `silver_product_subcategory`, `silver_product_category`. |
| **Business Logic** | `product_status`, `margin_pct` (carried through from Silver); category/subcategory names flattened via lookup join. |
| **Aggregations** | None — attribute-only dimension. |
| **Refresh Strategy** | Full overwrite from Silver each run. |

### `dim_territory`
| | |
|---|---|
| **Purpose** | Territory attributes for "Sales by Territory" KPIs. |
| **Grain** | One row per sales territory, **plus one manually-inserted "Unknown Territory" row** (`territory_key = -1`). |
| **Primary Key** | `territory_key` (= `territory_id`, or `-1` for Unknown). |
| **Foreign Keys** | None (country name denormalized flat). |
| **Source Silver** | `silver_sales_territory`, `silver_country_region`. |
| **Business Logic** | None beyond flattening country name onto the territory row. |
| **Aggregations** | None — revenue/order/customer counts are computed against `fact_sales`/`dim_customer`, never stored here. |
| **Refresh Strategy** | Full overwrite from Silver each run. |

### `dim_salesperson`
| | |
|---|---|
| **Purpose** | Salesperson attributes; supports salesperson-level slicing of revenue if ever needed beyond the stated KPIs. |
| **Grain** | One row per salesperson, **plus one "Unknown Salesperson" row** (`salesperson_key = -1`, for orders with no assigned rep — e.g. online orders). |
| **Primary Key** | `salesperson_key` (= `business_entity_id`, or `-1`). |
| **Foreign Keys** | None (territory name denormalized flat). |
| **Source Silver** | `silver_sales_person`, `silver_employee`, `silver_person`, `silver_sales_territory` (flat attribute only). |
| **Business Logic** | `quota_attainment_pct` carried from Silver; `is_current_employee` from `employee.current_flag`. |
| **Aggregations** | None. |
| **Refresh Strategy** | Full overwrite from Silver each run. |

### `dim_customer`
| | |
|---|---|
| **Purpose** | Customer attributes for "Customer Analytics" KPIs (total/active customers, customer type, customers by territory). |
| **Grain** | One row per customer. |
| **Primary Key** | `customer_key` (= `customer_id`). |
| **Foreign Keys** | None (territory name/group denormalized flat as `customer_territory_name`/`customer_territory_group`, from `customer.territory_id` — the customer's *registered* territory, which is a distinct concept from the *transaction's* territory on `fact_sales`; both are modeled, deliberately, because "customers by territory" and "revenue by territory" are different KPI questions that can legitimately disagree). |
| **Source Silver** | `silver_customer`, `silver_person`, `silver_store`, `silver_business_entity_address` (resolved to one primary address per customer — prefer the "Main Office"/"Home" address type, else the first by `address_id`, since a customer can have multiple addresses in the bridge table), `silver_address`, `silver_state_province`, `silver_country_region`, `silver_sales_territory`, and **`silver_sales_order_header`** (for the activity attributes below — Silver, not Gold, per the no-cross-Gold-dependency principle). |
| **Business Logic** | `customer_type`, `customer_name` (person's `full_name` or store's `store_name`), `email` (enriched from `bronze.emailaddress`), primary address fields. |
| **Aggregations** | `is_active_customer` (placed an order within the trailing 12 months **of the most recent order date in the data**, not real-world `current_date()` — AdventureWorks is a static historical dataset, not a live feed, so "active" must be relative to the data's own timeline), `first_order_date`, `most_recent_order_date`. Deliberately **excludes** `lifetime_revenue`/`lifetime_order_count` — those are fact-table aggregates that belong in a query-time join or a future summary table, not baked redundantly onto the dimension (a classic Kimball anti-pattern to avoid). |
| **Refresh Strategy** | Full overwrite from Silver each run. |

### `fact_sales`
| | |
|---|---|
| **Purpose** | The one source of truth for revenue, order, and units-sold KPIs. |
| **Grain** | **One row per sales order line item** (`silver_sales_order_detail` row) — the finest grain available, rolling up cleanly to order/customer/product/territory/date for every stated KPI. |
| **Primary Key** | `sales_order_detail_id` (globally unique across the whole table, not reset per order — confirmed from the source schema, so no composite key is needed). |
| **Foreign Keys** | `order_date_key`, `due_date_key`, `ship_date_key` (nullable) → `dim_date`; `customer_key` → `dim_customer`; `product_key` → `dim_product`; `salesperson_key` → `dim_salesperson` (`-1` when unresolved); `territory_key` → `dim_territory` (`-1` when unresolved). |
| **Source Silver** | `silver_sales_order_header` (order-level context) joined to `silver_sales_order_detail` (line-level measures). |
| **Business Logic** | `COALESCE(territory_id, -1)`, `COALESCE(sales_person_id, -1)` before the dimension join, so no fact row is ever dropped or left with a dangling FK. |
| **Aggregations** | None stored — `order_qty`, `unit_price`, `unit_price_discount`, `line_total` are additive measures aggregated at query time. |
| **Refresh Strategy** | Full overwrite from Silver each run. **`tax_amt`/`freight` are deliberately excluded** — they exist at order (header) grain, and allocating them down to line grain would require a fabricated pro-rata business rule nobody asked for; out of scope for the stated KPIs. |

### KPI → schema coverage check

| KPI | Query shape |
|---|---|
| Total Sales Revenue | `SUM(fact_sales.line_total)`, sliceable by any `dim_date` grain |
| Total Orders | `COUNT(DISTINCT fact_sales.sales_order_id)` |
| Total / Active Customers | `COUNT(*)` / `COUNT(* WHERE is_active_customer)` on `dim_customer` |
| Customer Type | `GROUP BY dim_customer.customer_type` |
| Customers by Territory | `GROUP BY dim_customer.customer_territory_name` |
| Top Products / Revenue by Product | `SUM(fact_sales.line_total) GROUP BY dim_product.product_name` |
| Units Sold | `SUM(fact_sales.order_qty) GROUP BY dim_product` |
| Sales by Territory — Revenue/Orders | `SUM(line_total)` / `COUNT(DISTINCT sales_order_id) GROUP BY dim_territory` (via `fact_sales.territory_key`, the *transaction's* territory) |
| Sales by Territory — Customers | `COUNT(DISTINCT fact_sales.customer_key) GROUP BY dim_territory` |

Every stated KPI is covered by exactly one join pattern — no KPI requires a snowflake join or a second fact table.

---

## 3. Table Dependencies

```
Bronze (existing, unmodified)
  customer, person, store, businessentityaddress, address, stateprovince, countryregion,
  product, productsubcategory, productcategory,
  salesperson, employee, salesterritory,
  sales_order_header, sales_order_detail
        |
        v
Silver (15 tables, 5 independently-runnable groups)
  Group A: silver_country_region, silver_state_province, silver_address,
           silver_business_entity_address                [no dependencies]
  Group B: silver_product_category, silver_product_subcategory, silver_product
           [product <- subcategory <- category, within Bronze itself]
  Group C: silver_person, silver_store, silver_customer   [customer needs person+store logically,
                                                            but reads Bronze directly — no Silver-to-
                                                            Silver read dependency at build time]
  Group D: silver_employee, silver_sales_person, silver_sales_territory  [no dependencies]
  Group E: silver_sales_order_header, silver_sales_order_detail
           [soft dependency: FK-validation step is more useful once Groups B/C/D already
            exist, but does not hard-block on them]
        |
        v
Gold (6 objects, ALL build independently from Silver — no Gold-to-Gold dependency)
  dim_date            <- (generated, no source dependency)
  dim_product         <- silver_product, silver_product_subcategory, silver_product_category
  dim_territory       <- silver_sales_territory, silver_country_region
  dim_salesperson     <- silver_sales_person, silver_employee, silver_person, silver_sales_territory
  dim_customer        <- silver_customer, silver_person, silver_store, silver_business_entity_address,
                          silver_address, silver_state_province, silver_country_region,
                          silver_sales_territory, silver_sales_order_header
  fact_sales          <- silver_sales_order_header, silver_sales_order_detail
```

No Silver table reads another Silver table, and no Gold table reads another Gold table. Every Silver table reads only Bronze; every Gold table reads only Silver. This is a deliberate simplification (documented in §2) that maximizes parallelism and keeps every notebook's failure blast radius contained to itself.

---

## 4. Transformation Flow

```
Bronze Delta Tables
        |
        v
Silver: clean, standardize types, dedupe, apply business rules, rename, add derived columns
        |  (full overwrite; audit columns: silver_load_timestamp, source_bronze_table)
        v
Silver Delta Tables (aide.silver.*)
        |
        v
Gold: join, denormalize (flatten hierarchies), resolve unknown-member FKs, compute
      dimension/fact grain
        |  (full overwrite; each object built independently from Silver)
        v
Gold Delta Tables (aide.gold.*)  — fact_sales + 5 dimensions
        |
        v
[Future] Streamlit reads aide.gold.* only — never Bronze/Silver directly
[Future] Business Review Workflow annotates Gold objects via a separate side table,
         never by adding governance columns onto the dimensional model itself
```

---

## 5. Best Practices Applied

- **Medallion discipline**: Bronze = raw fidelity (untouched), Silver = clean/conformed at *source grain*, Gold = business-consumable/denormalized at *query grain*. Denormalization happens once, at Gold — never in Silver.
- **Idempotency**: every layer uses full overwrite, matching the static, full-refresh nature of the AdventureWorks source. The MERGE/upsert pattern is documented as the production upgrade path for a real CDC-fed source, not built speculatively now.
- **No dangling FKs**: every dimension carries an "Unknown" member; every fact build coalesces nullable FKs before the join.
- **Star, not snowflake**: no dimension-to-dimension foreign keys; hierarchy is always flattened.
- **Dimensions hold attributes, not measures**: source pre-aggregates (`sales_ytd` etc.) are retained in Silver for audit but never trusted as a second source of truth in Gold.
- **Verify, don't assume**: every business rule in this document (person type codes, order status codes, PK uniqueness of `sales_order_detail_id`) was checked against `instawdb.sql` directly, not recalled from general AdventureWorks familiarity.
- **Fail-soft data quality**: FK/business-rule violations are logged as data-quality signals, not hard failures — consistent with every prior phase of this project (Bronze `ALL` mode, metadata collection, AI analysis) and directly useful input to the future AI Data Quality & Root Cause Analyzer phase.
- **Governance stays out of the model**: future Business Review/Approval/Comments/Active-Inactive-flag features attach to Gold objects via a separate metadata-schema side table (mirroring how `ai_analysis` already sits beside `table_metadata` rather than inside it) — Gold's dimensional model stays a clean system of record.

---

## 6. Folder Structure

```
notebooks/
  00_shared/
    01_gemini_client.py            (existing, untouched)
    02_silver_common.py            (NEW — generic reusable Silver helpers, see §7)
    03_gold_common.py              (NEW — generic reusable Gold helpers: unknown-member
                                     insertion, surrogate-key resolution, audit columns)
  01_development/                  (existing Bronze notebook — untouched, not renamed)
  02_ai_metadata/                  (existing — untouched)
  02_silver/                       (NEW)
    01_silver_reference_data.py    (country_region, state_province, address,
                                     business_entity_address)
    02_silver_customer_domain.py   (person, store, customer)
    03_silver_product_domain.py    (product, product_subcategory, product_category)
    04_silver_sales_org_domain.py  (employee, sales_person, sales_territory)
    05_silver_sales_transactions.py(sales_order_header, sales_order_detail)
  03_gold/                         (NEW)
    01_dim_date.py
    02_dim_product.py
    03_dim_territory.py
    04_dim_salesperson.py
    05_dim_customer.py
    06_fact_sales.py
```

**Note on numbering:** `02_ai_metadata` and the new `02_silver` share the `02_` prefix. I'm deliberately **not** renaming the existing, working `01_development`/`02_ai_metadata` folders to fix this — they're marked "do not modify," and a folder rename is unnecessary churn on something already complete (Databricks doesn't care about the prefix; it's purely for human sorting). A future cosmetic cleanup (e.g. renaming `02_ai_metadata` → `04_ai_metadata`, which nothing references by path, so it's safe whenever you want it) is worth doing eventually but isn't blocking anything here.

`config/`: no new registry file is proposed for Silver/Gold. Bronze needed `adventureworks_tables.yaml` because the *parsing* problem (headerless files, odd delimiters) was genuinely generic across tables. Silver/Gold transformations are business-rule-specific per table — a config file would just be a second, harder-to-read copy of what's already clearly expressed as code in each domain notebook.

---

## 7. Notebook Execution Order

1. **Silver — parallelizable**: `02_silver/01_silver_reference_data.py`, `02_silver_customer_domain.py`, `03_silver_product_domain.py`, `04_silver_sales_org_domain.py` can all run in any order or in parallel (each reads Bronze directly, no Silver-to-Silver dependency).
2. **Silver — last**: `05_silver_sales_transactions.py` — no hard dependency on step 1, but its FK-validation logging is more meaningful once the other Silver tables exist, so it's sequenced last by convention.
3. **Gold — fully parallelizable**: all six `03_gold/*.py` notebooks read Silver only and can run in any order or in parallel. Convention (not requirement): build dimensions before the fact, so that if fact-to-dim referential checks are ever added later, the dimensions are already there.
4. **[Future]** Streamlit app reads `aide.gold.*`.
5. **[Future]** Business Review Workflow runs after Gold, annotating Gold objects via its own side table.

---

## 8. PySpark Implementation Strategy

Same architectural DNA as the existing Bronze/AI notebooks — modular helper functions, typed exceptions, logging, a `main()` orchestrator, processing kept separate from persistence:

- **`00_shared/02_silver_common.py`** (generic, reused by every Silver domain notebook):
  `drop_technical_columns(df, cols)`, `trim_all_strings(df)`, `flags_to_boolean(df, flag_columns)`, `dedupe_on_key(df, key_cols, order_by_col="modified_date")`, `add_audit_columns(df, source_bronze_table)`.
- **`00_shared/03_gold_common.py`** (generic, reused by every Gold notebook):
  `add_unknown_member(dim_df, key_col, defaults: dict)`, `resolve_fk_or_unknown(fact_df, fk_col)` (wraps the `COALESCE(..., -1)` pattern), `ensure_schema_exists(spark, catalog, schema)`.
- Each **Silver domain notebook**: read Bronze table(s) via `spark.table(...)` → apply shared generic cleaners → apply table-specific business rules (written directly in the notebook — business rules are logic, not data, so they don't belong in a config file) → `saveAsTable(mode="overwrite", overwriteSchema=True)`.
- Each **Gold notebook**: read required Silver tables → join/denormalize/flatten → compute derived attributes → resolve unknown members → `saveAsTable(mode="overwrite", overwriteSchema=True)`.
- FK/business-rule validation: a small `log_fk_violations(df, fk_col, valid_keys_df)` helper that counts and logs violations (never hard-fails), consistent with the fail-soft philosophy already applied everywhere else in this project.
- Dimension tables are small enough that Spark's cost-based optimizer will auto-broadcast them in the `fact_sales` join; an explicit `broadcast()` hint is still worth adding for determinism rather than relying on the auto-broadcast threshold.

---

## 9. Suggested Delta Optimizations

- **Clustering on `fact_sales`**: recommend `CLUSTER BY (order_date_key, territory_key)` (Databricks Liquid Clustering) as the primary access pattern for date-range + territory-filtered BI queries — the modern replacement for manual `Z-ORDER`. *Caveat: verify Liquid Clustering syntax/availability against your actual DBR version before implementing — this recommendation is based on my training data, not something I can check against your workspace from here.* `ZORDER BY (order_date_key, territory_key)` is the fallback if Liquid Clustering isn't available.
- **Do not partition `fact_sales` by year**: at AdventureWorks' actual scale (~121K `SalesOrderDetail` rows), partitioning would create the small-file problem, not solve one. Partitioning by year becomes appropriate if/when this pipeline points at a real, much larger sales fact table — noted as the production trigger condition, not applied speculatively now.
- **`delta.autoOptimize.optimizeWrite` / `autoCompact` = true** on `fact_sales` and any Gold table refreshed incrementally in the future (the same small-file concern already flagged and accepted for `ai_analysis`'s per-table appends).
- **`OPTIMIZE`/`VACUUM`** on a periodic schedule (weekly is plenty at this data volume); default 7-day retention is fine unless time-travel needs differ.
- **Column stats**: all Silver/Gold tables here are narrow enough (well under Delta's default 32-column stats collection limit) that this needs no special configuration.

---

## 10. Future Scope — how this design keeps the door open

| Future item | How this design supports it without rework |
|---|---|
| AI Metadata Analysis (extended) | Already built for Bronze (`02_metadata_collector.py`/`03_metadata_analyzer.py`). Pointing it at `aide.silver`/`aide.gold` later is a config/widget change, not a redesign — Silver/Gold schemas are clean and well-documented specifically to make this easy. |
| Business Review Workflow, Approval Workflow, Business Comments, Active/Inactive flags | Modeled as a **separate side table** (e.g. `aide.metadata.business_review`, keyed by object name) referencing Gold objects — never as columns bolted onto the dimensional model. Mirrors the existing `ai_analysis` beside `table_metadata` pattern. |
| Streamlit Application | Consumes `aide.gold.*` exclusively — the star schema's flat, denormalized attributes mean every stated KPI is a single join, no complex query logic needed in the app layer. |
| Search / Data Catalog | Extends the already-built `table_metadata`/`ai_analysis` tables to cover Silver/Gold objects — no new infrastructure required. |

---

## Open questions for review before implementation

1. **Folder numbering**: comfortable with the `02_ai_metadata`/`02_silver` prefix overlap for now (§6), or would you rather I renumber immediately?
2. **`dim_customer`'s primary-address selection rule**: prefer "Main Office"/"Home" address type when a customer has multiple addresses in the bridge table, else first by `address_id` — confirm this is the right tiebreaker, or supply a different one.
3. **"Active Customer" window**: I used trailing 12 months from the data's own max order date. Confirm 12 months is the right window for this KPI.
4. **Liquid Clustering vs. Z-ORDER**: confirm your DBR version supports `CLUSTER BY` before I write it into the Gold notebooks as the primary recommendation.
