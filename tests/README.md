# tests/

Automated tests for the code in `src/`.

- `unit/` — fast, isolated tests (no Spark cluster, no live LLM calls — LLM/Spark dependencies are mocked)
- `integration/` — tests exercising real wiring (local Spark session, mocked or live LLM depending on suite)

No tests exist yet — folders are placeholders until `src/` has code to test.
