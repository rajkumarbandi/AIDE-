# notebooks/

Databricks notebooks. Notebooks here are orchestration-only: parse parameters, call into `src/`, log results. No business logic, prompt strings, or LLM calls should live directly in a notebook.

Organized by lifecycle phase:

- `01_development/` — AI Metadata Generator notebooks
- `02_testing/` — AI Data Quality & Root Cause Analyzer notebooks
- `03_deployment/` — AI Release Impact Analyzer notebooks

No notebooks exist yet.
