# AIDE — AI Data Engineering Assistant

An enterprise-grade accelerator demonstrating how Large Language Models (LLMs) can be integrated into the Data Engineering lifecycle on Databricks — not as a chatbot, but as reusable AI capabilities embedded in development, testing, and deployment workflows.

## Project Overview

AIDE is built around Microsoft's **AdventureWorks** sample database — a realistic enterprise OLTP dataset with multiple related business entities (Customer, Sales, Product, Employee, Vendor, Purchasing, and more). AdventureWorks tables are ingested into Databricks as Delta tables from day one, and every AI capability in this project operates against that Delta-based platform rather than ad-hoc files.

The project is currently in the **repository organization phase**: no ingestion code, notebooks, or prompts have been written yet. This README and folder structure exist to give the project a clean, scalable foundation before implementation begins.

## Project Vision

AIDE delivers three AI-assisted capabilities, one per phase of the Data Engineering lifecycle:

| Phase | Capability | What it does |
|---|---|---|
| **1. Development** | AI Metadata Generator | Given a newly onboarded table, automatically generates business/technical descriptions, column descriptions, candidate primary/foreign keys, business rules, PII detection, a data dictionary, and Markdown/JSON documentation. |
| **2. Testing** | AI Data Quality & Root Cause Analyzer | Given Spark profiling results on Bronze/Silver data, generates an executive summary, a data quality score, business impact and severity assessment, root cause analysis, and suggested SQL/PySpark fixes. |
| **3. Deployment** | AI Release Impact Analyzer | Given schema and code changes prior to release, identifies breaking changes, downstream impact, affected pipelines, risk level, regression test suggestions, and rollback recommendations. |

All three capabilities are designed to be **table-agnostic and metadata-driven** — onboarding a new AdventureWorks table (or, eventually, a table from any source) should require a configuration change, not new code.

## Technology Stack

- **Databricks** — compute platform, Unity Catalog governance
- **PySpark** — data profiling, schema introspection, diffing
- **Delta Lake** — Bronze/Silver storage and all AI-generated output tables
- **Unity Catalog** — catalog/schema governance, Volumes for raw data landing
- **Python** — core implementation language
- **Azure OpenAI / OpenAI API** — LLM provider(s), accessed through a provider-agnostic client
- **GitHub** — source control
- **GitHub Actions** *(planned)* — CI/CD

## Repository Structure

```
AIDE-/
├── data/
│   └── adventureworks/        # Raw AdventureWorks CSVs + install SQL (source dataset)
├── src/                       # Installable Python source code (business/AI/Spark logic)
│   └── utils/                 # Reusable cross-cutting utilities
├── notebooks/                 # Thin Databricks orchestration notebooks, per phase
│   ├── 01_development/
│   ├── 02_testing/
│   └── 03_deployment/
├── prompts/                   # AI prompt templates, per capability (separate from code)
│   ├── metadata_generator/
│   ├── dq_rca_analyzer/
│   └── release_impact_analyzer/
├── config/                    # Non-secret configuration (YAML); secrets via Secret Scopes
├── reports/                   # Generated human-readable reports (Markdown/HTML summaries)
├── outputs/                   # Raw AI-generated JSON output, plus samples/ (committed examples)
├── docs/                      # Architecture notes, diagrams, ADRs
├── tests/                     # unit/ and integration/ tests for src/
└── scripts/                   # Setup/operational scripts (not pipeline business logic)
```

Each folder contains its own `README.md` explaining its purpose in more detail.

## Development Roadmap

- [x] **Stage 0 — Repository organization**: folder structure, dataset placement, documentation (this stage)
- [ ] **Stage 1 — Platform foundation**: config loading, logging, secrets resolution, provider-agnostic LLM client, output-schema contracts, PII guardrails
- [ ] **Stage 2 — Ingestion**: table-registry-driven CSV → Volume → Bronze Delta ingestion for AdventureWorks tables
- [ ] **Stage 3 — Phase 1: AI Metadata Generator**: end-to-end on Bronze AdventureWorks tables, writing to metadata Delta tables
- [ ] **Stage 4 — Phase 2: AI Data Quality & Root Cause Analyzer**: Spark profiling, intentional DQ-issue injection on AdventureWorks copies, RCA generation
- [ ] **Stage 5 — Phase 3: AI Release Impact Analyzer**: schema evolution simulation on AdventureWorks tables, breaking-change/risk analysis
- [ ] **Stage 6 — CI/CD**: GitHub Actions for lint/test/deploy via Databricks Asset Bundles

## Future Enhancements

- Prompt evaluation harness (golden-dataset regression testing before promoting a prompt/model version)
- Cost and token usage tracking per capability, per environment
- Human-in-the-loop review gate for AI-generated metadata and release decisions
- Unity Catalog tagging/comment sync so AI-generated documentation is visible directly in the catalog UI
- Support for onboarding data sources beyond AdventureWorks once the platform is proven
- LLM provider fallback/circuit-breaker for graceful degradation on API outages

## Contributing

This project is in early-stage development. See each folder's `README.md` for the intended contents before adding new files.
