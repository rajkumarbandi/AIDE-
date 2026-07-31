# AIDE Streamlit Application

The AI Data Engineering Assistant's front end — reads the `gold`/`silver`/`bronze`/`metadata`
schemas built by `notebooks/`. Runs as a standalone app (outside Databricks), connecting via
`databricks-sql-connector` and the Gemini API.

## Status

**Foundation phase.** Navigation, theme, reusable components, and the AI Data Model Explorer's
graph rendering are implemented. Most pages have real (if minimal) queries wired up; deeper
business logic, trend analysis, and the full AI capability set (Generate SQL, Impact Analysis,
Explain this relationship) are the next implementation phase — see the "Phase 2" notes inside
`pages/03_AI_Data_Model_Explorer.py`.

## Run locally

```
cd streamlit
pip install -r requirements.txt
streamlit run app.py
```

## Configure secrets

Two ways to configure credentials — checked in this order (see `utils/secrets.py`):

1. **Environment variables** (used by Azure Databricks Apps — see `app.yaml`):
   `DATABRICKS_SERVER_HOSTNAME`, `DATABRICKS_HTTP_PATH`, `DATABRICKS_TOKEN`, `GEMINI_API_KEY`.
2. **`.streamlit/secrets.toml`** (local development; already `.gitignore`d — never commit this file):

```toml
[databricks]
server_hostname = "<workspace-hostname>"
http_path = "<sql-warehouse-http-path>"
access_token = "<personal-access-token-or-service-principal-token>"

[gemini]
api_key = "<gemini-api-key>"
```

Without either, the app still runs — every page falls back to an empty/error state explaining
what to configure, rather than crashing (see `⚙ Settings`).

## Deploying to Azure Databricks Apps

`app.yaml` (repo root of this folder) tells Databricks Apps to run `streamlit run app.py` — without
it, Databricks defaults to running the first `.py` file with plain `python`, which does not
bootstrap Streamlit's script runner correctly. Bind `DATABRICKS_SERVER_HOSTNAME`,
`DATABRICKS_HTTP_PATH`, `DATABRICKS_TOKEN`, and `GEMINI_API_KEY` as resources/secrets in the
Databricks Apps UI (or `databricks.yml`) — `app.yaml`'s `valueFrom` entries reference them by name.

Navigation uses Streamlit's classic filename-based multipage (the `pages/` directory), not
`st.Page()`/`st.navigation()` — the newer navigation API crashed on Databricks Apps with
`AttributeError: 'StreamlitPage' object has no attribute '_default'`. `streamlit` is pinned to an
exact version in `requirements.txt` for the same reason: Databricks Apps pre-installs its own
Streamlit version, and a loose `>=` floor that the pre-installed version already satisfies is not
guaranteed to be upgraded by `pip`, which would silently run this app against an untested version.

## Project structure

```
streamlit/
  app.py                 # entry point: redirects to the default page (classic multipage)
  app.yaml                # Azure Databricks Apps deployment manifest
  pages/                  # one file per sidebar page (classic filename-based multipage)
  components/             # reusable UI pieces (shell, header, KPI cards, tables, charts, graph, chat...)
  utils/                  # config, Databricks connector, Gemini client, SQL query templates, secrets
  assets/                 # logo, stylesheet
  .streamlit/config.toml  # dark theme base
```

Every page in `pages/` calls `components.shell.render_app_shell()` as its first statement — classic
multipage runs each page as an independent script (unlike `st.navigation()`, which re-ran shared
setup code from `app.py` on every navigation), so the page config/theme/sidebar that used to live
only in `app.py` is now a shared function every page calls for itself.
