# prompts/

AI prompt templates, kept as plain content separate from Python code so they can be reviewed and versioned independently of `src/`.

Organized by capability:

- `metadata_generator/` — prompts for Phase 1 (table/column descriptions, PII detection, key candidates, business rules)
- `dq_rca_analyzer/` — prompts for Phase 2 (root cause analysis, severity, suggested fixes)
- `release_impact_analyzer/` — prompts for Phase 3 (breaking-change detection, risk assessment, rollback recommendation)

No prompt content exists yet — folders are placeholders for when prompt authoring begins.
