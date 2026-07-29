# AdventureWorks Dataset

Raw AdventureWorks OLTP sample data used as the source dataset for AIDE.

## Contents

- `*.csv` — one file per AdventureWorks table (Customer, Product, SalesOrderHeader, SalesOrderDetail, Employee, Person, Vendor, PurchaseOrderHeader, etc.)
- `instawdb.sql` — Microsoft's original AdventureWorks OLTP install/schema script, kept for reference on table definitions, primary keys, and foreign keys (useful as ground truth when evaluating AI-generated metadata later).

## Usage

These files are the landing input for the future Bronze ingestion pipeline:

```
data/adventureworks/*.csv  →  Databricks Volume  →  Bronze Delta tables
```

No ingestion code exists yet — this folder currently only holds the raw source files.

## Note

This folder is ~92MB. If it grows significantly (e.g. additional AdventureWorks variants), consider Git LFS.
