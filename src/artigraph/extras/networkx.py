from collections import defaultdict
from typing import Any, Iterator, Mapping, Sequence
from uuid import UUID

import networkx as nx

from artigraph.core.api.artifact import Artifact
from artigraph.core.api.base import GraphObject
from artigraph.core.api.filter import LinkFilter, NodeFilter, NodeTypeFilter
from artigraph.core.api.funcs import read
from artigraph.core.api.link import Link
from artigraph.core.api.node import Node
from artigraph.core.model.base import GraphModel
from artigraph.core.orm.artifact import OrmArtifact, OrmModelArtifact
from artigraph.core.utils.anysync import anysync

_NodesById = Mapping[UUID, GraphObject]
_NodeRelationships = Mapping[UUID, Sequence[UUID]]
_LabelsById = Mapping[UUID, str | None]


@anysync
async def create_graph(root: GraphObject) -> nx.DiGraph:
    """Create a NetworkX graph from an Artigraph node."""
    nodes_by_id, relationship, labels = await _read_nodes_relationships_labels(root)

    graph = nx.DiGraph()
    for i, n in _dfs_iter_nodes(root, list(nodes_by_id.values()), relationship):
        graph.add_node(i, obj=n, label=labels.get(i))
    graph.add_edges_from(
        [
            (source, target, {"label": labels[target]})
            for source, targets in relationship.items()
            for target in targets
        ]
    )

    for layer, nodes in enumerate(nx.topological_generations(graph)):
        # `multipartite_layout` expects the layer as a node attribute, so add the
        # numeric layer value as a node attribute
        for node in nodes:
            graph.nodes[node]["subset"] = layer

    return graph


async def _read_nodes_relationships_labels(
    root: GraphObject,
) -> tuple[_NodesById, _NodeRelationships, _LabelsById]:
    links = await read.a(
        Link,
        LinkFilter(ancestor=root.graph_id),
    )
    nodes = await read.a(
        Node,
        NodeFilter(
            id=[l.target_id for l in links],
            node_type=NodeTypeFilter(
                subclasses=True,
                not_type=OrmArtifact,
            ),
        ),
    )
    artifacts = await read.a(
        Artifact,
        NodeFilter(
            id=[l.target_id for l in links],
            node_type=NodeTypeFilter(
                subclasses=True,
                type=OrmArtifact,
                not_type=OrmModelArtifact,
            ),
        ),
    )
    models = await read.a(
        GraphModel,
        NodeFilter(
            id=[l.target_id for l in links],
            node_type=NodeTypeFilter(
                subclasses=True,
                type=OrmModelArtifact,
            ),
        ),
    )

    relationships: defaultdict[UUID, list[UUID]] = defaultdict(list)
    for l in links:
        relationships[l.source_id].append(l.target_id)

    labels = {l.target_id: l.label for l in links}

    nodes_by_id: dict[UUID, GraphObject] = {}
    node_likes: Sequence[GraphObject] = [*nodes, *artifacts, *models]
    for n in node_likes:
        nodes_by_id[n.graph_id] = n

    return nodes_by_id, relationships, labels


def _dfs_iter_nodes(
    root: GraphObject,
    nodes: Sequence[GraphObject],
    relationships: _NodeRelationships,
) -> Iterator[tuple[UUID, GraphObject]]:
    """Yield nodes in depth-first order."""
    nodes_by_id: dict[UUID, GraphObject] = {}
    for n in nodes:
        nodes_by_id[n.graph_id] = n

    seen = set()
    stack: list[tuple[UUID, Any]] = [(root.graph_id, root)]
    while stack:
        obj_id, obj = stack.pop()
        seen.add(obj_id)
        yield obj_id, obj
        for target_id in relationships[obj_id]:
            if target_id not in seen:
                stack.append((target_id, nodes_by_id[target_id]))
            else:  # nocov
                # recursive query in create_graph prevents us from ever seeing circular graphs
                pass
