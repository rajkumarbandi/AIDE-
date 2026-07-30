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

Create `.streamlit/secrets.toml` (already `.gitignore`d — never commit this file):

```toml
[databricks]
server_hostname = "<workspace-hostname>"
http_path = "<sql-warehouse-http-path>"
access_token = "<personal-access-token-or-service-principal-token>"

[gemini]
api_key = "<gemini-api-key>"
```

Without these, the app still runs — every page falls back to an empty/error state explaining
what to configure, rather than crashing (see `⚙ Settings`).

## Project structure

```
streamlit/
  app.py                 # entry point: page config, theme, sidebar shell, routing
  pages/                 # one file per sidebar page (st.Page-based navigation)
  components/            # reusable UI pieces (header, KPI cards, tables, charts, graph, chat...)
  utils/                 # config, Databricks connector, Gemini client, SQL query templates
  assets/                # logo, stylesheet
  .streamlit/config.toml # dark theme base
```
