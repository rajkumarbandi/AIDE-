"""AI Data Model Explorer's ER diagram engine.

Renders entity-relationship boxes (table name + PK/FK, per real database-
documentation conventions) instead of plain circular nodes — via Plotly
shapes (the boxes) + annotations (the text) + an invisible marker trace per
box for click/hover, layered on NetworkX only for the left-to-right
Bronze/Silver/Gold positioning. Same library choice as before (see git
history): Plotly is Streamlit-native and gives real pan/zoom for free, so
switching to a box-based look needed no new dependency.

Connectors are straight lines between box edges (not full orthogonal
right-angle routing like diagrams.net/Lucidchart) — box-edge-to-box-edge
straight connectors are a real, common ER-diagram style, achievable within
Plotly's shape system without a new charting dependency; true orthogonal
edge routing would need one.
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
# Widened from the original 0/1/2 spacing to leave clear room for boxes.
_LAYER_X = {"bronze": 0, "silver": 2.6, "metadata": 2.6, "gold": 5.2}
_ROW_SPACING = 1.3
_BOX_WIDTH = 1.9
_BOX_HEIGHT = 0.85
_HEADER_FRACTION = 0.34  # top slice of the box reserved for the table-name band
_HIGHLIGHT_COLOR = "#2563eb"
_MUTED_EDGE_COLOR = "#4b5563"
_HEADER_FILL = "#0e1117"
_MAX_KEY_LINE_ITEMS = 3
_STAR_SPOKE_SPACING = _BOX_WIDTH + 0.7
_STAR_SPOKE_ROW_Y = _ROW_SPACING * 1.8


def build_graph(nodes: list, edges: list) -> nx.DiGraph:
    """nodes: [{"id", "label", "layer", "kind", "pk": [...], "fk": [...]}, ...]
    ("pk"/"fk" are optional — an empty/missing list just shows no key line.)
    edges: [{"source", "target", "label"}, ...]
    """
    graph = nx.DiGraph()
    for node in nodes:
        graph.add_node(node["id"], **node)
    for edge in edges:
        graph.add_edge(edge["source"], edge["target"], label=edge.get("label", ""))
    return graph


def _layered_positions(graph: nx.DiGraph) -> dict:
    """Deterministic left-to-right-by-layer, stacked-within-layer box-center positions."""
    layer_buckets = {}
    for node_id, data in graph.nodes(data=True):
        layer_buckets.setdefault(data.get("layer", "bronze"), []).append(node_id)

    positions = {}
    for layer, node_ids in layer_buckets.items():
        x = _LAYER_X.get(layer, 0)
        for i, node_id in enumerate(sorted(node_ids)):
            positions[node_id] = (x, -i * _ROW_SPACING)
    return positions


def _star_positions(graph: nx.DiGraph) -> dict:
    """A clean hub-and-spokes layout for a small, focused subgraph (e.g. one
    fact table + its dimensions) — the hub (highest-degree node) sits at the
    origin; every other node is spread evenly along a single row above it.
    Every spoke connects only to the hub, never to another spoke, so this
    layout has zero line crossings by construction, unlike a generic
    force-directed graph — exactly the "clean, professional ERD" quality bar
    a 60+ table full-lineage view can't realistically hit at that density.
    """
    if not graph.nodes:
        return {}
    hub = max(graph.nodes, key=lambda n: graph.degree(n))
    spokes = sorted(n for n in graph.nodes if n != hub)

    positions = {hub: (0.0, 0.0)}
    count = len(spokes)
    start_x = -(count - 1) * _STAR_SPOKE_SPACING / 2
    for i, node_id in enumerate(spokes):
        positions[node_id] = (start_x + i * _STAR_SPOKE_SPACING, _STAR_SPOKE_ROW_Y)
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


def _key_line(items: list, icon: str) -> str:
    if not items:
        return ""
    shown = items[:_MAX_KEY_LINE_ITEMS]
    suffix = f" +{len(items) - _MAX_KEY_LINE_ITEMS} more" if len(items) > _MAX_KEY_LINE_ITEMS else ""
    return f"{icon} {', '.join(shown)}{suffix}"


def _box_edge_anchor(x0: float, y0: float, x1: float, y1: float) -> tuple:
    """Where a straight connector should touch each box's border, not its
    center — comparing horizontal vs. vertical separation to decide whether
    to anchor at the left/right edges (cross-layer edges) or top/bottom
    edges (same-layer, vertically-stacked edges like fact_sales' FKs to its
    Gold-layer dimensions).
    """
    dx, dy = x1 - x0, y1 - y0
    if abs(dx) >= abs(dy):
        start = (x0 + _BOX_WIDTH / 2 if dx > 0 else x0 - _BOX_WIDTH / 2, y0)
        end = (x1 - _BOX_WIDTH / 2 if dx > 0 else x1 + _BOX_WIDTH / 2, y1)
    else:
        start = (x0, y0 + _BOX_HEIGHT / 2 if dy > 0 else y0 - _BOX_HEIGHT / 2)
        end = (x1, y1 - _BOX_HEIGHT / 2 if dy > 0 else y1 + _BOX_HEIGHT / 2)
    return start, end


def render_graph(
    graph: nx.DiGraph,
    key: str = "aide_warehouse_graph",
    highlight_node: Optional[str] = None,
    layout: str = "layered",
) -> Optional[dict]:
    """Render the ER diagram. Returns {"type": "node", "id": ...} or
    {"type": "edge", "source": ..., "target": ...} for whatever was clicked,
    or None if nothing is selected this run.

    `layout`: "layered" (default — Bronze/Silver/Gold columns, for the full
    lineage view) or "star" (a hub + its direct neighbors only, laid out via
    _star_positions — the focused, no-crossings view for a single fact table
    and its dimensions).

    If `highlight_node` is set, edges touching that node (and that node's own
    box border) are drawn in the accent color and thicker; everything else
    stays muted — the "click table to highlight relationships" behavior.

    Cardinality is labeled at each real FK edge's endpoints ("1" nearest the
    referenced/parent table, "N" nearest the referencing/child table) — a
    Plotly-native substitute for crow's-foot notation glyphs, which Plotly's
    shape system has no built-in support for. Bronze->Silver->Gold lineage
    ("feeds") edges are not foreign keys and are never labeled with a
    cardinality, which would otherwise be a fabricated claim.
    """
    positions = _star_positions(graph) if layout == "star" else _layered_positions(graph)
    node_ids = list(graph.nodes())

    shapes = []
    annotations = []
    edge_traces = []
    edge_midpoint_x, edge_midpoint_y, edge_midpoint_ids = [], [], []

    for source, target in graph.edges():
        x0, y0 = positions[source]
        x1, y1 = positions[target]
        is_highlighted = highlight_node is not None and highlight_node in (source, target)
        start, end = _box_edge_anchor(x0, y0, x1, y1)
        edge_traces.append(
            go.Scatter(
                x=[start[0], end[0]],
                y=[start[1], end[1]],
                mode="lines",
                line=dict(
                    width=3 if is_highlighted else 1.5,
                    color=_HIGHLIGHT_COLOR if is_highlighted else _MUTED_EDGE_COLOR,
                ),
                hoverinfo="none",
                showlegend=False,
            )
        )
        edge_midpoint_x.append((start[0] + end[0]) / 2)
        edge_midpoint_y.append((start[1] + end[1]) / 2)
        edge_midpoint_ids.append(f"{source}::{target}")

        # Cardinality labels only apply to real FK edges (GOLD_RELATIONSHIPS —
        # `target` is the referenced/parent "1" side, `source` the referencing/
        # child "N" side). A bronze->silver->gold lineage ("feeds") edge isn't a
        # foreign key at all, so labeling it with a cardinality would be a
        # fabricated claim, not a fact — it's deliberately skipped.
        if graph.edges[source, target].get("label") != "feeds":
            label_color = _HIGHLIGHT_COLOR if is_highlighted else "#9aa4b2"
            near_start = (start[0] + (end[0] - start[0]) * 0.12, start[1] + (end[1] - start[1]) * 0.12)
            near_end = (end[0] + (start[0] - end[0]) * 0.12, end[1] + (start[1] - end[1]) * 0.12)
            annotations.append(
                dict(x=near_start[0], y=near_start[1], text="N", showarrow=False,
                     font=dict(color=label_color, size=11, family="monospace"))
            )
            annotations.append(
                dict(x=near_end[0], y=near_end[1], text="1", showarrow=False,
                     font=dict(color=label_color, size=11, family="monospace"))
            )

    edge_click_trace = go.Scatter(
        x=edge_midpoint_x,
        y=edge_midpoint_y,
        mode="markers",
        marker=dict(size=12, color="rgba(0,0,0,0)"),
        customdata=edge_midpoint_ids,
        hoverinfo="skip",
        showlegend=False,
        name="edges",
    )

    node_hover_x, node_hover_y, node_hovertext = [], [], []
    for node_id in node_ids:
        data = graph.nodes[node_id]
        x, y = positions[node_id]
        is_highlighted = node_id == highlight_node
        color = _node_color(data)
        border_color = _HIGHLIGHT_COLOR if is_highlighted else "#0e1117"
        border_width = 3 if is_highlighted else 1.5

        box_top = y + _BOX_HEIGHT / 2
        box_bottom = y - _BOX_HEIGHT / 2
        header_bottom = box_top - _BOX_HEIGHT * _HEADER_FRACTION

        # Body first (fill=layer/kind color), then a distinct, darker header
        # band on top of it — a real database-documentation entity box has a
        # visually separate title bar, not just a plain colored rectangle.
        shapes.append(
            dict(
                type="rect",
                x0=x - _BOX_WIDTH / 2, x1=x + _BOX_WIDTH / 2,
                y0=box_bottom, y1=box_top,
                line=dict(color=border_color, width=border_width),
                fillcolor=color,
                opacity=0.85 if is_highlighted else 0.55,
                layer="below",
            )
        )
        shapes.append(
            dict(
                type="rect",
                x0=x - _BOX_WIDTH / 2, x1=x + _BOX_WIDTH / 2,
                y0=header_bottom, y1=box_top,
                line=dict(color=border_color, width=border_width),
                fillcolor=_HEADER_FILL,
                opacity=0.92,
                layer="below",
            )
        )

        annotations.append(
            dict(
                x=x, y=(header_bottom + box_top) / 2,
                text=f"<b>{data.get('label', node_id)}</b>",
                showarrow=False,
                font=dict(color="#f5f5f5", size=12),
                align="center",
            )
        )

        pk_line = _key_line(data.get("pk") or [], "🔑")
        fk_line = _key_line(data.get("fk") or [], "🔗")
        body_lines = []
        if pk_line:
            body_lines.append(pk_line)
        if fk_line:
            body_lines.append(fk_line)
        if not body_lines:
            body_lines.append("<i>no keys collected</i>")

        annotations.append(
            dict(
                x=x, y=(box_bottom + header_bottom) / 2,
                text="<br>".join(body_lines),
                showarrow=False,
                font=dict(color="#f5f5f5", size=11),
                align="center",
            )
        )

        node_hover_x.append(x)
        node_hover_y.append(y)
        all_pk = ", ".join(data.get("pk") or []) or "—"
        all_fk = ", ".join(data.get("fk") or []) or "—"
        node_hovertext.append(
            f"<b>{data.get('label', node_id)}</b><br>Layer: {data.get('layer', '—')}<br>"
            f"Kind: {data.get('kind', '—')}<br>Primary Key(s): {all_pk}<br>Foreign Key(s): {all_fk}"
        )

    node_click_trace = go.Scatter(
        x=node_hover_x,
        y=node_hover_y,
        mode="markers",
        marker=dict(size=[_BOX_WIDTH * 32] * len(node_ids), color="rgba(0,0,0,0)"),
        customdata=node_ids,
        hovertext=node_hovertext,
        hoverinfo="text",
        showlegend=False,
        name="nodes",
    )

    fig = go.Figure(data=edge_traces + [edge_click_trace, node_click_trace])
    fig.update_layout(
        shapes=shapes,
        annotations=annotations,
        showlegend=False,
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        xaxis=dict(visible=False, fixedrange=False),
        yaxis=dict(visible=False, fixedrange=False, scaleanchor="x"),
        margin=dict(l=10, r=10, t=10, b=10),
        height=600,
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
    if curve_number == edge_trace_count + 1:  # node_click_trace is the last one added
        if 0 <= point_index < len(node_ids):
            return {"type": "node", "id": node_ids[point_index]}
    return None
