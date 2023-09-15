import networkx as nx

from artigraph.core.api.artifact import Artifact
from artigraph.core.api.filter import ArtifactFilter, NodeFilter, NodeLinkFilter, NodeTypeFilter
from artigraph.core.api.funcs import read
from artigraph.core.api.link import NodeLink
from artigraph.core.api.node import Node
from artigraph.core.orm.artifact import OrmArtifact
from artigraph.core.utils.anysync import anysync


@anysync
async def create_graph(root: Node) -> nx.DiGraph:
    """Create a NetworkX graph from an Artigraph node."""
    links = await read.a(NodeLink, NodeLinkFilter(ancestor=root.node_id))
    nodes = await read.a(
        Node,
        NodeFilter(
            node_id=[l.child_id for l in links],
            node_type=NodeTypeFilter(subclasses=True, not_type=OrmArtifact),
        ),
    )
    artifacts = await read.a(
        Artifact,
        ArtifactFilter(node_id=[l.child_id for l in links]),
    )
    node_labels = {l.child_id: l.label for l in links}

    graph = nx.DiGraph()
    for n in [root, *nodes, *artifacts]:
        graph.add_node(n.node_id, obj=n, label=node_labels.get(n.node_id))
    graph.add_edges_from([(l.parent_id, l.child_id, {"label": l.label}) for l in links])

    return graph


def display_graph(graph: nx.Graph) -> None:
    errors: list[ImportError] = []
    try:
        _display_plotly_graph(graph)
    except ImportError as e:
        errors.append(e)
    else:
        return

    msg = "Could not display graph. Please install one of the following packages:"
    for e in errors:
        msg += f"\n- {e.name}"
    raise ImportError(msg)


def _display_plotly_graph(graph: nx.Graph, hover_text_line_limit: int = 25) -> None:
    import plotly.graph_objects as go

    try:
        import pandas as pd
    except ImportError:
        pass
    else:
        pd.set_option("display.max_rows", 20)

    for layer, nodes in enumerate(nx.topological_generations(graph)):
        # `multipartite_layout` expects the layer as a node attribute, so add the
        # numeric layer value as a node attribute
        for node in nodes:
            graph.nodes[node]["subset"] = layer

    pos = nx.multipartite_layout(graph)

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
            "color": [],
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

    fig = go.Figure(
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

    fig.show()


def _create_node_hover_text(label: str | None, node: Node | Artifact) -> str:
    text = _html_title("LABEL")
    text += f"{label}<br>"

    if isinstance(node, Artifact):
        text += f"<br>{_html_title('VALUE')}{node.value!r}"

    return text.replace("\n", "<br>")


def _html_title(name: str) -> str:
    return f"<br>{name}<br>" + ("-" * len(name)) + "<br>"
