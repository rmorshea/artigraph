from __future__ import annotations

from artigraph.core.api.filter import LinkFilter, NodeFilter
from artigraph.core.api.funcs import delete_one, exists, read_one, write_many
from artigraph.core.api.link import Link
from artigraph.core.api.node import Node


async def test_write_read_delete_node():
    """Test writing, reading, and deleting a node."""
    info = await create_graph()
    parent1 = info["parent1"]

    node_filter = NodeFilter(id=parent1.graph_id)
    node_link_filter = LinkFilter(parent=parent1.graph_id)

    parent = await read_one.a(Node, node_filter)
    assert parent.graph_id == parent1.graph_id
    assert await exists.a(Link, node_link_filter)

    await delete_one.a(parent)
    assert not await exists.a(Node, node_filter)
    assert not await exists.a(Link, node_link_filter)


async def create_graph() -> dict[str, Node]:
    """Create a simple tree of nodes.

    The tree looks like this:

    grandparent
    ├── parent
    │   ├── child1
    │   │   └── grandchild1
    │   └── child2
    ├── parent2
    │  ├── child3
    │  └── child4
    └── parent3
    """

    node_info = {}

    grandparent = Node()
    parent1 = Node()
    child1 = Node()
    grandchild1 = Node()
    child2 = Node()
    parent2 = Node()
    child3 = Node()
    child4 = Node()
    parent3 = Node()

    node_info["grandparent"] = grandparent
    node_info["parent1"] = parent1
    node_info["child1"] = child1
    node_info["grandchild1"] = grandchild1
    node_info["child2"] = child2
    node_info["parent2"] = parent2
    node_info["child3"] = child3
    node_info["child4"] = child4
    node_info["parent3"] = parent3

    grandparent_to_parent = Link(
        source_id=grandparent.graph_id,
        target_id=parent1.graph_id,
        label="grandparent_to_1parent",
    )
    parent_to_child1 = Link(
        source_id=parent1.graph_id,
        target_id=child1.graph_id,
        label="parent1_to_child1",
    )
    child1_to_grandchild1 = Link(
        source_id=child1.graph_id,
        target_id=grandchild1.graph_id,
        label="child1_to_grandchild1",
    )
    parent_to_child2 = Link(
        source_id=parent1.graph_id,
        target_id=child2.graph_id,
        label="parent1_to_child2",
    )
    grandparent_to_parent2 = Link(
        source_id=grandparent.graph_id,
        target_id=parent2.graph_id,
        label="grandparent_to_parent2",
    )
    parent2_to_child3 = Link(
        source_id=parent2.graph_id,
        target_id=child3.graph_id,
        label="parent2_to_child3",
    )
    parent2_to_child4 = Link(
        source_id=parent2.graph_id,
        target_id=child4.graph_id,
        label="parent2_to_child4",
    )
    grandparent_to_parent3 = Link(
        source_id=grandparent.graph_id,
        target_id=parent3.graph_id,
        label="grandparent_to_parent3",
    )

    await write_many.a(
        [
            grandparent,
            parent1,
            child1,
            grandchild1,
            child2,
            parent2,
            child3,
            child4,
            parent3,
            grandparent_to_parent,
            parent_to_child1,
            child1_to_grandchild1,
            parent_to_child2,
            grandparent_to_parent2,
            parent2_to_child3,
            parent2_to_child4,
            grandparent_to_parent3,
        ]
    )

    return node_info
