from typing import Iterator

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
    for n in _dfs_iter_nodes(root, [*nodes, *artifacts], links):
        graph.add_node(n.node_id, obj=n, label=node_labels.get(n.node_id))
    graph.add_edges_from([(l.parent_id, l.child_id, {"label": l.label}) for l in links])

    for layer, nodes in enumerate(nx.topological_generations(graph)):
        # `multipartite_layout` expects the layer as a node attribute, so add the
        # numeric layer value as a node attribute
        for node in nodes:
            graph.nodes[node]["subset"] = layer

    return graph


def _dfs_iter_nodes(
    root: Node, nodes: list[Node | Artifact], links: list[NodeLink]
) -> Iterator[Node | Artifact]:
    """Yield nodes in depth-first order."""
    seen = set()
    stack = [root]
    while stack:
        node = stack.pop()
        if node.node_id in seen:
            continue
        seen.add(node.node_id)
        yield node
        for link in links:
            if link.parent_id == node.node_id:
                stack.append(next(n for n in nodes if n.node_id == link.child_id))
