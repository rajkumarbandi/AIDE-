# config/

Non-secret configuration (YAML), kept separate from implementation code so environments can be reconfigured without touching `src/`.

## Contents

- `adventureworks_tables.yaml` — the AdventureWorks table registry used by `notebooks/01_development/01_bronze_ingestion.py`: per-table delimiter, verified column names/types. Onboarding a new AdventureWorks table into Bronze ingestion means adding an entry here, not changing the notebook.

Environment-layered config (`base.yaml` + `dev.yaml`/`staging.yaml`/`prod.yaml`) is planned but not created yet.

**Secrets never live here.** API keys, connection strings, and endpoints resolve at runtime via Databricks Secret Scopes / Azure Key Vault, never from files in this folder.
