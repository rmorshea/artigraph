from __future__ import annotations

from typing import TYPE_CHECKING

import plotly.graph_objects as go
from plotly import io as plotly_io
from plotly.graph_objs import Figure, FigureWidget

from artigraph.core.api.artifact import Artifact
from artigraph.core.api.node import Node
from artigraph.core.model.base import GraphModel
from artigraph.core.serializer.base import Serializer

if TYPE_CHECKING:
    import networkx as nx


class FigureJsonSerializer(Serializer[Figure | FigureWidget]):
    """Serialize a plotly figure"""

    name = "artigraph-plotly-figure-json"
    types = (Figure, FigureWidget)

    def serialize(self, figure: Figure | FigureWidget) -> bytes:
        result = plotly_io.to_json(figure)
        if result is None:  # no cov
            msg = "Plotly failed to serialize the figure - this is likely an issue with Plotly"
            raise RuntimeError(msg)
        return result.encode()

    def deserialize(self, data: bytes) -> Figure | FigureWidget:
        return plotly_io.from_json(data.decode())


figure_json_serializer = FigureJsonSerializer().register()
"""Serialize a plotly figure"""


def figure_from_networkx(graph: nx.Graph, hover_text_line_limit: int = 25) -> go.Figure:
    """Create a figure from a NetworkX graph"""
    import networkx as nx

    try:
        import pandas as pd
    except ImportError:  # nocov
        pass
    else:
        pd.set_option("display.max_rows", 20)

    pos = nx.multipartite_layout(graph, align="horizontal")

    node_x = []
    node_y = []
    for node in graph.nodes():
        x, y = pos[node]
        node_x.append(x)
        node_y.append(y)

    edge_x = []
    edge_y = []
    for edge in graph.edges():
        x0, y0 = pos[edge[0]]
        x1, y1 = pos[edge[1]]
        edge_x.extend([x0, x1, None])
        edge_y.extend([y0, y1, None])

    # color Artifact green and Node blue
    node_colors = [
        "blue"  # Deep Blue
        if isinstance(graph.nodes[node]["obj"], Artifact)
        else "yellow"  # Bright Yellow
        if isinstance(graph.nodes[node]["obj"], GraphModel)
        else "green"  # Teal
        for node in graph.nodes()
    ]

    edge_trace = go.Scatter(
        x=edge_x,
        y=edge_y,
        mode="lines",
        line={"width": 3},
    )

    node_trace = go.Scatter(
        x=node_x,
        y=node_y,
        mode="markers",
        hoverinfo="text",
        marker={
            "color": node_colors,
            "size": 12,
            "line_width": 2,
        },
    )

    node_text: list[str] = []
    # generate node text from label of parent edge
    for node in graph.nodes():
        node_attrs = graph.nodes[node]
        text = _create_node_hover_text(node_attrs["label"], node_attrs["obj"])
        br_count = text.count("<br>")
        if br_count > hover_text_line_limit:
            text = "<br>".join(text.split("<br>")[:25] + ["<br>..."])
        node_text.append(text)
    node_trace.text = node_text

    return go.Figure(
        data=[edge_trace, node_trace],
        layout=go.Layout(
            titlefont_size=16,
            showlegend=False,
            hovermode="closest",
            xaxis={"showgrid": False, "zeroline": False, "showticklabels": False},
            yaxis={"showgrid": False, "zeroline": False, "showticklabels": False},
            hoverlabel={"font": {"family": "monospace", "size": 10}},
        ),
    )


def _create_node_hover_text(label: str | None, node: Node | Artifact | GraphModel) -> str:
    text = _html_title("LABEL")
    text += f"{label}<br>"

    if isinstance(node, Artifact):
        text += f"<br>{_html_title('VALUE')}{node.value!r}"
    elif isinstance(node, GraphModel):
        text += f"<br>{_html_title('MODEL')}{node.graph_model_name} v{node.graph_model_version}"

    return text.replace("\n", "<br>")


def _html_title(name: str) -> str:
    return f"<br>{name}<br>" + ("-" * len(name)) + "<br>"
