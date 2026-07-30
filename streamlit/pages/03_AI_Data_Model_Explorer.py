"""AI Data Model Explorer — the flagship feature.

An interactive Bronze -> Silver -> Gold warehouse graph. Nodes and edges are
built from real sources only (see "Do not fake relationships" in the brief):
  - Nodes: metadata.table_metadata (Bronze, live query) unioned with the known
    Silver/Gold table names (real — from having built those notebooks — but
    not yet AI-analyzed; see utils/queries.py KNOWN_SILVER_TABLES/KNOWN_GOLD_TABLES).
  - Edges: utils/queries.py's SILVER_LINEAGE, GOLD_LINEAGE, and
    GOLD_RELATIONSHIPS — each one a direct transcription of a real
    read_bronze_table()/read_silver_table()/join() call in the actual
    Silver/Gold notebooks, not invented.

Foundation scope: the graph itself, PK/FK/lineage/sample-SQL derivation, and
"Explain this table" (reusing already-stored AI analysis) are real and
working now. "Explain this relationship", "Generate SQL", and "Impact
Analysis" are scaffolded as clearly-labeled Phase 2 capabilities — they need
either a live Gemini call or warehouse-wide dependency analysis this
foundation phase intentionally doesn't build yet.
"""

from typing import Optional

import networkx as nx
import pandas as pd
import streamlit as st

from components.filters import get_filters
from components.graph import build_graph, render_graph, render_legend
from components.header import render_page_header
from components.tables import render_empty_state
from utils.databricks import DatabricksConnectionError, DatabricksQueryError, run_query
from utils.queries import (
    GOLD_LINEAGE,
    GOLD_RELATIONSHIPS,
    KNOWN_GOLD_TABLES,
    KNOWN_SILVER_TABLES,
    SILVER_LINEAGE,
    sql_ai_analysis,
    sql_table_metadata,
)

render_page_header(
    title="AI Data Model Explorer",
    description="An interactive Bronze → Silver → Gold warehouse graph — click any "
    "table to see its AI-generated description, keys, relationships, and lineage.",
    breadcrumb=["Home", "AI Data Model Explorer"],
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
            }
    for table_name in KNOWN_SILVER_TABLES:
        nodes.setdefault(
            table_name, {"id": table_name, "label": table_name, "layer": "silver", "kind": "table"}
        )
    for table_name in KNOWN_GOLD_TABLES:
        kind = "fact" if table_name.startswith("fact_") else "dimension"
        nodes[table_name] = {"id": table_name, "label": table_name, "layer": "gold", "kind": kind}
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


def _render_node_detail(
    graph: nx.DiGraph, node_id: str, metadata_df: Optional[pd.DataFrame], catalog: str
) -> None:
    node_data = graph.nodes[node_id]
    st.subheader(f"📄 {node_id}")
    st.caption(f"Layer: {node_data.get('layer', '—')} · Kind: {node_data.get('kind', '—')}")

    meta_row = None
    if metadata_df is not None and not metadata_df.empty:
        matches = metadata_df[metadata_df["table_name"] == node_id]
        if not matches.empty:
            meta_row = matches.iloc[0]

    tabs = st.tabs(
        [
            "AI Description", "Keys & Relationships", "Lineage", "Suggested SQL",
            "Common KPIs & Use Cases",
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
        if meta_row is not None:
            pk_candidates = meta_row.get("primary_key_candidates") or "—"
            st.markdown(f"**Primary Key Candidates:** {pk_candidates}")
        else:
            st.markdown("**Primary Key Candidates:** _not yet collected for this table_")
        related = _related_tables(graph, node_id)
        st.markdown(f"**Related Tables:** {', '.join(related) if related else '—'}")
        st.markdown("**Relationships:**")
        for rel in GOLD_RELATIONSHIPS:
            if node_id in (rel["from_table"], rel["to_table"]):
                st.markdown(f"- `{rel['from_table']}.{rel['from_column']}` → "
                            f"`{rel['to_table']}.{rel['to_column']}` — {rel['description']}")

    with tabs[2]:
        upstream = _upstream_lineage(graph, node_id)
        downstream = _downstream_lineage(graph, node_id)
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("**⬆ Upstream Lineage**")
            if upstream:
                upstream_text = "\n".join(f"- {t}" for t in upstream)
            else:
                upstream_text = "_None — this is a source table._"
            st.markdown(upstream_text)
        with col2:
            st.markdown("**⬇ Downstream Lineage**")
            if downstream:
                downstream_text = "\n".join(f"- {t}" for t in downstream)
            else:
                downstream_text = "_Nothing depends on this table yet._"
            st.markdown(downstream_text)

    with tabs[3]:
        joins = _suggested_join_sql(graph, node_id)
        if joins:
            for sql_text in joins:
                st.code(sql_text, language="sql")
        else:
            st.code(f"SELECT * FROM {catalog}.<schema>.{node_id} LIMIT 100", language="sql")

    with tabs[4]:
        st.info("Common KPIs and business use cases will be AI-generated in the next "
                "implementation phase, using this table's stored ai_analysis content "
                "plus its graph position.")


def _render_edge_detail(rel_source: str, rel_target: str) -> None:
    st.subheader(f"🔗 {rel_source} → {rel_target}")
    matching = [
        rel for rel in GOLD_RELATIONSHIPS
        if rel["from_table"] == rel_source and rel["to_table"] == rel_target
    ]
    if matching:
        rel = matching[0]
        st.markdown(rel["description"])
        st.code(
            f"SELECT *\nFROM {rel['from_table']} f\nJOIN {rel['to_table']} d\n"
            f"  ON f.{rel['from_column']} = d.{rel['to_column']}",
            language="sql",
        )
    else:
        st.markdown("This is a Bronze → Silver or Silver → Gold lineage edge — the source "
                    "table feeds the target table's transformation.")

    st.markdown("**Impact Analysis** _(coming in the next phase)_")
    st.caption(
        "Will show: if a column on the source table changes, which downstream tables, "
        "dashboards, and KPIs are affected."
    )


filters = get_filters()
render_legend()

metadata_df = _load_bronze_metadata(filters["catalog"])
if metadata_df is None:
    st.caption("⚠ Not connected to Databricks — showing the known Silver/Gold warehouse "
               "structure only, without live Bronze metadata. Connect in ⚙ Settings for "
               "the full picture.")

nodes = _build_nodes(metadata_df)
node_ids = {n["id"] for n in nodes}
edges = _build_edges(node_ids)

if not nodes:
    render_empty_state("No warehouse structure available yet.", icon="🧠")
else:
    graph = build_graph(nodes, edges)
    selected_node = st.session_state.get("aide_selected_node")

    selection = render_graph(graph, highlight_node=selected_node)
    if selection:
        if selection["type"] == "node":
            st.session_state["aide_selected_node"] = selection["id"]
            st.session_state.pop("aide_selected_edge", None)
        elif selection["type"] == "edge":
            st.session_state["aide_selected_edge"] = (selection["source"], selection["target"])
            st.session_state.pop("aide_selected_node", None)

    st.divider()

    if st.session_state.get("aide_selected_edge"):
        source, target = st.session_state["aide_selected_edge"]
        _render_edge_detail(source, target)
    elif st.session_state.get("aide_selected_node"):
        selected_id = st.session_state["aide_selected_node"]
        _render_node_detail(graph, selected_id, metadata_df, filters["catalog"])
    else:
        st.caption("👆 Click any table above to see its details, or click a connecting "
                   "line to see that relationship.")
