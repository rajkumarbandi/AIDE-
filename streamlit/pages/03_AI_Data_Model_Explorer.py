"""AI Data Model Explorer — the flagship feature.

An interactive Bronze -> Silver -> Gold warehouse graph. Nodes and edges are
built from real sources only:
  - Nodes: metadata.table_metadata (Bronze, live query) unioned with the known
    Silver/Gold table names (real — from having built those notebooks — but
    not yet AI-analyzed; see utils/queries.py KNOWN_SILVER_TABLES/KNOWN_GOLD_TABLES).
  - Edges: utils/queries.py's SILVER_LINEAGE, GOLD_LINEAGE, and
    GOLD_RELATIONSHIPS — each one a direct transcription of a real
    read_bronze_table()/read_silver_table()/join() call in the actual
    Silver/Gold notebooks, not invented.

Search filters the graph to matching tables plus their direct neighbors.
Layer checkboxes give a real collapse/expand mechanic. Node detail covers
description, live columns, keys/relationships, lineage, a real (non-AI)
impact analysis derived from the dependency graph, a live data preview, and
suggested SQL. "Explain this relationship" is a genuine Gemini call grounded
in the real relationship description, clearly distinguished from the
deterministic graph facts around it.
"""

from typing import Optional

import networkx as nx
import pandas as pd
import streamlit as st

from components.filters import get_filters
from components.graph import build_graph, render_graph, render_legend
from components.header import render_page_header
from components.shell import render_app_shell
from components.tables import render_dataframe, render_empty_state
from utils.config import DEFAULT_BRONZE_SCHEMA, DEFAULT_GOLD_SCHEMA, DEFAULT_SILVER_SCHEMA
from utils.databricks import DatabricksConnectionError, DatabricksQueryError, run_query
from utils.gemini import GeminiClientError, GeminiConfigurationError, explain_relationship
from utils.helpers import to_plain_list
from utils.queries import (
    GOLD_LINEAGE,
    GOLD_RELATIONSHIPS,
    KNOWN_GOLD_TABLES,
    KNOWN_SILVER_TABLES,
    SILVER_LINEAGE,
    gold_keys_for_table,
    sql_ai_analysis,
    sql_information_schema_columns,
    sql_preview_table,
    sql_table_metadata,
)

render_app_shell()

render_page_header(
    title="AI Data Model Explorer",
    description="An interactive Bronze → Silver → Gold warehouse graph — click any "
    "table to see its AI-generated description, keys, relationships, lineage, and "
    "impact analysis.",
    breadcrumb=["Home", "AI Data Model Explorer"],
)

_LAYER_SCHEMAS = {
    "bronze": DEFAULT_BRONZE_SCHEMA,
    "silver": DEFAULT_SILVER_SCHEMA,
    "gold": DEFAULT_GOLD_SCHEMA,
}

# fact_sales + these four dims are the real, verified inputs to
# 03_gold/07_sales_dashboard.py's sales_kpi_summary build (compute_* functions
# read exactly these five DataFrames) — used for a grounded, non-fabricated
# Impact Analysis statement, not a guess.
_KPI_DEPENDENT_TABLES = frozenset(
    {"fact_sales", "dim_customer", "dim_product", "dim_territory", "dim_salesperson"}
)


@st.cache_data(ttl=300, show_spinner=False)
def _load_bronze_metadata(catalog: str) -> Optional[pd.DataFrame]:
    try:
        return run_query(sql_table_metadata(catalog=catalog))
    except (DatabricksConnectionError, DatabricksQueryError):
        return None


def _build_nodes(metadata_df: Optional[pd.DataFrame]) -> list:
    """Bronze nodes come from live metadata when available; Silver/Gold nodes
    are always added from the known table lists (see module docstring).

    "pk"/"fk" are populated for the ER diagram's key display: Bronze uses
    the AI-collected primary_key_candidates; Gold uses gold_keys_for_table()
    (the real GOLD_RELATIONSHIPS registry). Silver has no live FK metadata
    to read here (Unity Catalog declares none), so it shows no key line —
    an honest gap, not a fabricated one.
    """
    nodes = {}
    if metadata_df is not None and not metadata_df.empty:
        for _, row in metadata_df.iterrows():
            table_name = row["table_name"]
            nodes[table_name] = {
                "id": table_name,
                "label": table_name,
                "layer": row.get("schema_name", "bronze"),
                "kind": "table",
                "pk": to_plain_list(row.get("primary_key_candidates")),
                "fk": [],
            }
    for table_name in KNOWN_SILVER_TABLES:
        nodes.setdefault(
            table_name,
            {"id": table_name, "label": table_name, "layer": "silver", "kind": "table", "pk": [], "fk": []},
        )
    for table_name in KNOWN_GOLD_TABLES:
        kind = "fact" if table_name.startswith("fact_") else "dimension"
        primary_keys, foreign_keys = gold_keys_for_table(table_name)
        nodes[table_name] = {
            "id": table_name,
            "label": table_name,
            "layer": "gold",
            "kind": kind,
            "pk": primary_keys,
            "fk": foreign_keys,
        }
    return list(nodes.values())


def _build_edges(node_ids: set) -> list:
    edges = []
    for silver_table, bronze_sources in SILVER_LINEAGE.items():
        for bronze_table in bronze_sources:
            if silver_table in node_ids and bronze_table in node_ids:
                edges.append({"source": bronze_table, "target": silver_table, "label": "feeds"})
    for gold_table, silver_sources in GOLD_LINEAGE.items():
        for silver_table in silver_sources:
            if gold_table in node_ids and silver_table in node_ids:
                edges.append({"source": silver_table, "target": gold_table, "label": "feeds"})
    for rel in GOLD_RELATIONSHIPS:
        if rel["from_table"] in node_ids and rel["to_table"] in node_ids:
            edges.append(
                {
                    "source": rel["from_table"],
                    "target": rel["to_table"],
                    "label": rel["description"],
                }
            )
    return edges


def _related_tables(graph: nx.DiGraph, node_id: str) -> list:
    return sorted(set(graph.predecessors(node_id)) | set(graph.successors(node_id)))


def _upstream_lineage(graph: nx.DiGraph, node_id: str) -> list:
    return sorted(nx.ancestors(graph, node_id))


def _downstream_lineage(graph: nx.DiGraph, node_id: str) -> list:
    return sorted(nx.descendants(graph, node_id))


def _suggested_join_sql(graph: nx.DiGraph, node_id: str) -> list:
    """Real, deterministic SQL derived from GOLD_RELATIONSHIPS — not AI-generated."""
    statements = []
    for rel in GOLD_RELATIONSHIPS:
        if rel["from_table"] == node_id:
            statements.append(
                f"SELECT *\nFROM {rel['from_table']} f\nJOIN {rel['to_table']} d\n"
                f"  ON f.{rel['from_column']} = d.{rel['to_column']}"
            )
        elif rel["to_table"] == node_id:
            statements.append(
                f"SELECT *\nFROM {rel['to_table']} d\nJOIN {rel['from_table']} f\n"
                f"  ON d.{rel['to_column']} = f.{rel['from_column']}"
            )
    return statements


def _filter_by_search(nodes: list, edges: list, search: str) -> tuple:
    """Reduce the graph to matching tables plus their direct (1-hop) neighbors —
    without this, a search on a 69+ table warehouse graph is nearly useless.
    """
    if not search:
        return nodes, edges
    node_ids = {n["id"] for n in nodes}
    matches = {nid for nid in node_ids if search.lower() in nid.lower()}
    if not matches:
        return [], []
    neighbors = set(matches)
    for edge in edges:
        if edge["source"] in matches:
            neighbors.add(edge["target"])
        if edge["target"] in matches:
            neighbors.add(edge["source"])
    filtered_nodes = [n for n in nodes if n["id"] in neighbors]
    filtered_edges = [e for e in edges if e["source"] in neighbors and e["target"] in neighbors]
    return filtered_nodes, filtered_edges


def _filter_by_layer(nodes: list, edges: list, visible_layers: set) -> tuple:
    """Collapse/expand mechanic: hide whole medallion layers."""
    filtered_nodes = [n for n in nodes if n.get("layer") in visible_layers]
    visible_ids = {n["id"] for n in filtered_nodes}
    filtered_edges = [e for e in edges if e["source"] in visible_ids and e["target"] in visible_ids]
    return filtered_nodes, filtered_edges


def _render_impact_analysis(graph: nx.DiGraph, node_id: str) -> None:
    downstream = _downstream_lineage(graph, node_id)
    if not downstream:
        render_empty_state(
            "Nothing downstream — this table is a terminal node in the current graph "
            "(nothing reads from it, per the lineage/relationship registries).",
            icon="✅",
        )
        return

    st.markdown(
        f"**{len(downstream)} table(s) would be affected** if `{node_id}`'s schema or "
        "contents changed — every table reachable from it in the Bronze→Silver→Gold "
        "dependency graph:"
    )
    for table in downstream:
        st.markdown(f"- `{table}`")

    if node_id in _KPI_DEPENDENT_TABLES or _KPI_DEPENDENT_TABLES & set(downstream):
        st.warning(
            "⚠ Includes `fact_sales` and/or its dimensions — a change here can reach "
            "`sales_kpi_summary` and the Executive Dashboard's headline KPIs "
            "(verified against 03_gold/07_sales_dashboard.py's actual inputs)."
        )


def _render_node_detail(
    graph: nx.DiGraph, node_id: str, metadata_df: Optional[pd.DataFrame], catalog: str
) -> None:
    node_data = graph.nodes[node_id]
    layer = node_data.get("layer", "bronze")
    st.subheader(f"📄 {node_id}")
    st.caption(f"Layer: {layer} · Kind: {node_data.get('kind', '—')}")

    meta_row = None
    if metadata_df is not None and not metadata_df.empty:
        matches = metadata_df[metadata_df["table_name"] == node_id]
        if not matches.empty:
            meta_row = matches.iloc[0]

    tabs = st.tabs(
        [
            "AI Description", "Columns", "Keys & Relationships", "Lineage",
            "Impact Analysis", "Data Preview", "Suggested SQL",
        ]
    )

    with tabs[0]:
        if st.button("🤖 Explain this table", key=f"explain_{node_id}"):
            try:
                analysis_df = run_query(sql_ai_analysis(catalog=catalog, table_name=node_id))
            except (DatabricksConnectionError, DatabricksQueryError) as exc:
                st.error(f"Could not load AI analysis: {exc}")
                analysis_df = None
            if analysis_df is not None and not analysis_df.empty:
                st.markdown(analysis_df.iloc[0].get("analysis_markdown") or "_No content stored._")
            else:
                render_empty_state(
                    f"No AI analysis stored yet for '{node_id}'. Run "
                    "03_metadata_analyzer_poc.py for this table.",
                    icon="🤖",
                )
        else:
            st.caption("Click to display the AI-generated business description already "
                       "stored in metadata.ai_analysis for this table.")

    with tabs[1]:
        schema = _LAYER_SCHEMAS.get(layer, DEFAULT_BRONZE_SCHEMA)
        try:
            columns_df = run_query(sql_information_schema_columns(catalog, schema, node_id))
            render_dataframe(columns_df, empty_message="No column metadata available.")
        except (DatabricksConnectionError, DatabricksQueryError) as exc:
            st.error(f"Could not load columns: {exc}")

    with tabs[2]:
        if meta_row is not None:
            pk_candidates = to_plain_list(meta_row.get("primary_key_candidates"))
            st.markdown(f"**Primary Key Candidates:** {', '.join(pk_candidates) or '—'}")
        else:
            st.markdown("**Primary Key Candidates:** _not yet collected for this table_")
        related = _related_tables(graph, node_id)
        st.markdown(f"**Related Tables:** {', '.join(related) if related else '—'}")
        st.markdown("**Relationships:**")
        has_relationship = False
        for rel in GOLD_RELATIONSHIPS:
            if node_id in (rel["from_table"], rel["to_table"]):
                has_relationship = True
                st.markdown(f"- `{rel['from_table']}.{rel['from_column']}` → "
                            f"`{rel['to_table']}.{rel['to_column']}` — {rel['description']}")
        if not has_relationship:
            st.caption("No declared Gold-layer relationships for this table.")

    with tabs[3]:
        upstream = _upstream_lineage(graph, node_id)
        downstream = _downstream_lineage(graph, node_id)
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("**⬆ Upstream Lineage**")
            if upstream:
                st.markdown("\n".join(f"- {t}" for t in upstream))
            else:
                st.markdown("_None — this is a source table._")
        with col2:
            st.markdown("**⬇ Downstream Lineage**")
            if downstream:
                st.markdown("\n".join(f"- {t}" for t in downstream))
            else:
                st.markdown("_Nothing depends on this table yet._")

    with tabs[4]:
        _render_impact_analysis(graph, node_id)

    with tabs[5]:
        schema = _LAYER_SCHEMAS.get(layer, DEFAULT_BRONZE_SCHEMA)
        try:
            preview_df = run_query(sql_preview_table(catalog, schema, node_id, limit=20))
            render_dataframe(preview_df, empty_message="This table has no rows yet.")
        except (DatabricksConnectionError, DatabricksQueryError) as exc:
            st.error(f"Could not load a data preview: {exc}")

    with tabs[6]:
        joins = _suggested_join_sql(graph, node_id)
        if joins:
            for sql_text in joins:
                st.code(sql_text, language="sql")
        else:
            schema = _LAYER_SCHEMAS.get(layer, DEFAULT_BRONZE_SCHEMA)
            st.code(f"SELECT * FROM {catalog}.{schema}.{node_id} LIMIT 100", language="sql")


def _render_edge_detail(rel_source: str, rel_target: str, graph: nx.DiGraph) -> None:
    st.subheader(f"🔗 {rel_source} → {rel_target}")
    matching = [
        rel for rel in GOLD_RELATIONSHIPS
        if rel["from_table"] == rel_source and rel["to_table"] == rel_target
    ]
    description = None
    if matching:
        rel = matching[0]
        description = rel["description"]
        join_condition = f"{rel['from_table']}.{rel['from_column']} = {rel['to_table']}.{rel['to_column']}"

        col1, col2 = st.columns(2)
        with col1:
            st.markdown(f"**Parent Table:** `{rel['to_table']}`")
            st.markdown("**Join Type:** LEFT JOIN")
            st.markdown(f"**Referenced Key:** `{rel['to_column']}`")
        with col2:
            st.markdown(f"**Child Table:** `{rel['from_table']}`")
            st.markdown(
                f"**Cardinality:** Many-to-One (many `{rel['from_table']}` rows per "
                f"`{rel['to_table']}` row)"
            )
        st.markdown(f"**Business Meaning:** {description}")

        st.markdown("**Join Condition** _(click to copy)_")
        st.code(join_condition, language="sql")

        st.markdown("**Generated Join SQL** _(click to copy)_")
        st.code(
            f"SELECT *\nFROM {rel['from_table']} f\nLEFT JOIN {rel['to_table']} d\n"
            f"  ON f.{rel['from_column']} = d.{rel['to_column']}",
            language="sql",
        )

        col_a, col_b, col_c = st.columns(3)
        with col_a:
            if st.button(f"📄 View {rel['to_table']}", key=f"view_parent_{rel_source}_{rel_target}"):
                st.session_state["aide_selected_node"] = rel["to_table"]
                st.session_state.pop("aide_selected_edge", None)
                st.rerun()
        with col_b:
            if st.button(f"📄 View {rel['from_table']}", key=f"view_child_{rel_source}_{rel_target}"):
                st.session_state["aide_selected_node"] = rel["from_table"]
                st.session_state.pop("aide_selected_edge", None)
                st.rerun()
        with col_c:
            if st.button("🔍 Highlight in Diagram", key=f"highlight_{rel_source}_{rel_target}"):
                st.session_state["aide_selected_node"] = rel["from_table"]
                st.session_state.pop("aide_selected_edge", None)
                st.rerun()
    else:
        description = (
            f"'{rel_source}' feeds '{rel_target}' as part of the medallion build — the "
            "source table's transformation output becomes this target table."
        )
        st.markdown("This is a Bronze → Silver or Silver → Gold lineage edge — the source "
                    "table feeds the target table's transformation. It has no join key of its "
                    "own (it isn't a foreign-key relationship, it's an ETL build step).")

    if st.button("🤖 Explain this relationship in plain English", key=f"explain_rel_{rel_source}_{rel_target}"):
        with st.spinner("Asking Gemini..."):
            try:
                st.info(explain_relationship(description))
            except GeminiConfigurationError:
                st.error("Gemini isn't configured yet — see ⚙ Settings.")
            except GeminiClientError as exc:
                st.error(f"Could not generate an explanation: {exc}")

    st.markdown("**Impact Analysis**")
    if rel_target in graph.nodes:
        _render_impact_analysis(graph, rel_target)


def _build_gold_star_graph() -> tuple:
    """Just the Gold fact table + its dimensions (KNOWN_GOLD_TABLES minus
    sales_kpi_summary, which has no FK edges) — a small, focused subgraph
    for the star-schema view. Real PK/FK from gold_keys_for_table(), same as
    the full view; no live Bronze metadata query needed for this view.
    """
    star_tables = [t for t in KNOWN_GOLD_TABLES]
    nodes = []
    for table_name in star_tables:
        kind = "fact" if table_name.startswith("fact_") else "dimension"
        primary_keys, foreign_keys = gold_keys_for_table(table_name)
        nodes.append(
            {
                "id": table_name, "label": table_name, "layer": "gold", "kind": kind,
                "pk": primary_keys, "fk": foreign_keys,
            }
        )
    node_ids = {n["id"] for n in nodes}
    edges = [
        {"source": rel["from_table"], "target": rel["to_table"], "label": rel["description"]}
        for rel in GOLD_RELATIONSHIPS
        if rel["from_table"] in node_ids and rel["to_table"] in node_ids
    ]
    return nodes, edges


filters = get_filters()

view_mode = st.radio(
    "View",
    ["⭐ Gold Star Schema", "🔗 Full Lineage View"],
    horizontal=True,
    key="aide_model_view_mode",
)

if view_mode == "⭐ Gold Star Schema":
    st.caption(
        "The Gold-layer fact table and its dimensions — a focused, no-crossings view. "
        "Switch to Full Lineage View to see Bronze → Silver → Gold and search across "
        "every table."
    )
    render_legend()
    nodes, edges = _build_gold_star_graph()
    metadata_df = None
    graph_layout = "star"
else:
    col_search, col_bronze, col_silver, col_gold = st.columns([3, 1, 1, 1])
    with col_search:
        search = st.text_input(
            "🔍 Search tables", placeholder="e.g. customer — shows matches + direct neighbors"
        )
    with col_bronze:
        show_bronze = st.checkbox("Bronze", value=True, key="aide_show_bronze")
    with col_silver:
        show_silver = st.checkbox("Silver", value=True, key="aide_show_silver")
    with col_gold:
        show_gold = st.checkbox("Gold", value=True, key="aide_show_gold")

    render_legend()

    metadata_df = _load_bronze_metadata(filters["catalog"])
    if metadata_df is None:
        st.caption("⚠ Not connected to Databricks — showing the known Silver/Gold warehouse "
                   "structure only, without live Bronze metadata. Connect in ⚙ Settings for "
                   "the full picture.")

    all_nodes = _build_nodes(metadata_df)
    all_node_ids = {n["id"] for n in all_nodes}
    all_edges = _build_edges(all_node_ids)

    visible_layers = {layer for layer, show in
                       [("bronze", show_bronze), ("silver", show_silver), ("gold", show_gold)] if show}
    nodes, edges = _filter_by_layer(all_nodes, all_edges, visible_layers)
    nodes, edges = _filter_by_search(nodes, edges, search)
    graph_layout = "layered"

if not nodes:
    render_empty_state("No tables match the current search/layer filters.", icon="🧠")
else:
    if view_mode == "🔗 Full Lineage View":
        st.caption(f"Showing {len(nodes)} of {len(all_nodes)} tables")
    graph = build_graph(nodes, edges)
    selected_node = st.session_state.get("aide_selected_node")

    selection = render_graph(graph, highlight_node=selected_node, layout=graph_layout)
    if selection:
        if selection["type"] == "node":
            st.session_state["aide_selected_node"] = selection["id"]
            st.session_state.pop("aide_selected_edge", None)
        elif selection["type"] == "edge":
            st.session_state["aide_selected_edge"] = (selection["source"], selection["target"])
            st.session_state.pop("aide_selected_node", None)

    st.divider()

    selected_edge = st.session_state.get("aide_selected_edge")
    selected_id = st.session_state.get("aide_selected_node")

    if selected_edge and selected_edge[0] in graph.nodes and selected_edge[1] in graph.nodes:
        source, target = selected_edge
        _render_edge_detail(source, target, graph)
    elif selected_id and selected_id in graph.nodes:
        _render_node_detail(graph, selected_id, metadata_df, filters["catalog"])
    else:
        st.caption("👆 Click any table above to see its details, or click a connecting "
                   "line to see that relationship.")
