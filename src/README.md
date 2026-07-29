# src/

Installable Python source code for AIDE. This is the only place business/AI/Spark logic will live — notebooks (`notebooks/`) stay thin and orchestration-only.

## Current contents

- `utils/` — reusable helpers shared across future capabilities (config loading, logging, path resolution, etc.)

## Planned (not created yet)

As implementation begins, this will grow into layered packages, e.g. `core/` (config, logging, secrets), `ai/` (LLM client, prompt loader, output schemas), `spark/` (profiling, diffing, Delta helpers), and `capabilities/` (metadata generator, DQ/RCA analyzer, release impact analyzer). No packages beyond `utils/` are scaffolded yet, by design — this task is repo organization only.
