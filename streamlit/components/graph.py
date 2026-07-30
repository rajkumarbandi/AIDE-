"""AI Data Model Explorer graph engine.

Library choice (researched, not assumed): NetworkX for layout only, Plotly for
rendering, via st.plotly_chart(..., on_select="rerun") — Streamlit's native
selection-event API. Two alternatives were considered and rejected:
- yFiles Graphs for Streamlit is the most polished option, but it's a
  commercial product; a licensing dependency isn't appropriate to bake into
  a portfolio project's foundation without an explicit decision to pay for it.
- streamlit-agraph is the most common dedicated graph component, but has
  documented compatibility issues following its 2.0 update (per Streamlit's
  own community discussion, mid-2026).
Plotly is Streamlit-native, actively maintained, and gives real pan/zoom/hover
for free — no extra component dependency, no version-compatibility risk.
"""

import logging
from typing import Optional

import networkx as nx
import plotly.graph_objects as go
import streamlit as st

from utils.config import LAYER_COLORS

logger = logging.getLogger("aide.graph")

# Left-to-right column per layer, so the layout visually reads as
# Bronze -> Silver -> Gold rather than a generic force-directed blob.
_LAYER_X = {"bronze": 0, "silver": 1, "metadata": 1, "gold": 2}
_HIGHLIGHT_COLOR = "#2563eb"
_MUTED_EDGE_COLOR = "#4b5563"


def build_graph(nodes: list, edges: list) -> nx.DiGraph:
    """nodes: [{"id", "label", "layer", "kind"}, ...]
    edges: [{"source", "target", "label"}, ...]
    """
    graph = nx.DiGraph()
    for node in nodes:
        graph.add_node(node["id"], **node)
    for edge in edges:
        graph.add_edge(edge["source"], edge["target"], label=edge.get("label", ""))
    return graph


def _layered_positions(graph: nx.DiGraph) -> dict:
    """Deterministic left-to-right-by-layer, stacked-within-layer positions."""
    layer_buckets = {}
    for node_id, data in graph.nodes(data=True):
        layer_buckets.setdefault(data.get("layer", "bronze"), []).append(node_id)

    positions = {}
    for layer, node_ids in layer_buckets.items():
        x = _LAYER_X.get(layer, 0)
        for i, node_id in enumerate(sorted(node_ids)):
            positions[node_id] = (x, -i)
    return positions


def _node_color(data: dict) -> str:
    """Fact/dimension/metadata color takes priority over the medallion layer
    color (a Gold fact table is colored as "fact", not generically "gold").
    """
    kind = data.get("kind")
    if kind in ("fact", "dimension"):
        return LAYER_COLORS[kind]
    return LAYER_COLORS.get(data.get("layer", "bronze"), LAYER_COLORS["bronze"])


def render_legend() -> None:
    """Small color-key legend, since a single multi-color Plotly trace can't
    auto-generate one.
    """
    items = "".join(
        f'<span style="margin-right:16px;"><span class="aide-status-dot" '
        f'style="background:{color};"></span>{label.title()}</span>'
        for label, color in LAYER_COLORS.items()
    )
    st.markdown(f'<div style="margin-bottom:8px;">{items}</div>', unsafe_allow_html=True)


def render_graph(
    graph: nx.DiGraph, key: str = "aide_warehouse_graph", highlight_node: Optional[str] = None
) -> Optional[dict]:
    """Render the graph. Returns {"type": "node", "id": ...} or
    {"type": "edge", "source": ..., "target": ...} for whatever was clicked,
    or None if nothing is selected this run.

    If `highlight_node` is set, edges touching that node are drawn in the
    accent color and thicker; all other edges stay muted — the
    "relationship highlighting" behavior.
    """
    positions = _layered_positions(graph)
    node_ids = list(graph.nodes())

    edge_traces = []
    edge_midpoint_x, edge_midpoint_y, edge_midpoint_ids = [], [], []
    for source, target in graph.edges():
        x0, y0 = positions[source]
        x1, y1 = positions[target]
        is_highlighted = highlight_node is not None and highlight_node in (source, target)
        edge_traces.append(
            go.Scatter(
                x=[x0, x1],
                y=[y0, y1],
                mode="lines",
                line=dict(
                    width=3 if is_highlighted else 1.5,
                    color=_HIGHLIGHT_COLOR if is_highlighted else _MUTED_EDGE_COLOR,
                ),
                hoverinfo="none",
                showlegend=False,
            )
        )
        edge_midpoint_x.append((x0 + x1) / 2)
        edge_midpoint_y.append((y0 + y1) / 2)
        edge_midpoint_ids.append(f"{source}::{target}")

    edge_click_trace = go.Scatter(
        x=edge_midpoint_x,
        y=edge_midpoint_y,
        mode="markers",
        marker=dict(size=10, color="rgba(0,0,0,0)"),
        customdata=edge_midpoint_ids,
        hoverinfo="skip",
        showlegend=False,
        name="edges",
    )

    node_x = [positions[n][0] for n in node_ids]
    node_y = [positions[n][1] for n in node_ids]
    node_trace = go.Scatter(
        x=node_x,
        y=node_y,
        mode="markers+text",
        text=[graph.nodes[n].get("label", n) for n in node_ids],
        textposition="top center",
        textfont=dict(color="#e6e6e6", size=11),
        marker=dict(
            size=[28 if n == highlight_node else 22 for n in node_ids],
            color=[_node_color(graph.nodes[n]) for n in node_ids],
            line=dict(
                width=[3 if n == highlight_node else 1 for n in node_ids],
                color="#0e1117",
            ),
        ),
        customdata=node_ids,
        hovertext=[
            f"{graph.nodes[n].get('label', n)} ({graph.nodes[n].get('layer', '')})"
            for n in node_ids
        ],
        hoverinfo="text",
        name="nodes",
    )

    fig = go.Figure(data=edge_traces + [edge_click_trace, node_trace])
    fig.update_layout(
        showlegend=False,
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        xaxis=dict(visible=False),
        yaxis=dict(visible=False),
        margin=dict(l=10, r=10, t=10, b=10),
        height=520,
        dragmode="pan",
    )

    event = st.plotly_chart(
        fig, width="stretch", key=key, on_select="rerun", selection_mode="points"
    )

    return _resolve_selection(event, node_ids, len(edge_traces))


def _resolve_selection(event, node_ids: list, edge_trace_count: int) -> Optional[dict]:
    """Map a plotly_chart selection event back to a node or edge id.

    Defensive about the event dict's exact shape (dict-style .get() throughout,
    multiple possible key-name fallbacks for the point index) since this
    couldn't be verified against a live Streamlit session from this sandbox.
    """
    points = (event or {}).get("selection", {}).get("points", [])
    if not points:
        return None
    point = points[0]

    custom = point.get("customdata")
    curve_number = point.get("curve_number", point.get("curveNumber"))

    # customdata directly tells us node id (string) vs edge id ("source::target").
    if isinstance(custom, str) and "::" in custom:
        source, target = custom.split("::", 1)
        return {"type": "edge", "source": source, "target": target}
    if custom is not None:
        return {"type": "node", "id": custom}

    # Fallback: use curve_number to know which trace, point_index for position.
    point_index = point.get("point_index", point.get("pointIndex", point.get("point_number")))
    if point_index is None:
        return None
    if curve_number == edge_trace_count + 1:  # node_trace is the last one added
        if 0 <= point_index < len(node_ids):
            return {"type": "node", "id": node_ids[point_index]}
    return None
