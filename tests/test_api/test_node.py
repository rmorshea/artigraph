from __future__ import annotations

from artigraph.api.filter import NodeFilter, NodeLinkFilter
from artigraph.api.funcs import delete_one, exists, read_one, write
from artigraph.api.link import NodeLink
from artigraph.api.node import Node


async def test_write_read_delete_node():
    """Test writing, reading, and deleting a node."""
    await create_graph()

    node_filter = NodeFilter(node_id="parent")
    node_link_filter = NodeLinkFilter(parent="parent") | NodeLinkFilter(child="parent")

    parent = await read_one(Node, node_filter)
    assert parent.node_id == "parent"
    assert await exists(NodeLink, node_link_filter)

    await delete_one(parent)
    assert not await exists(Node, node_filter)
    assert not await exists(NodeLink, node_link_filter)


async def create_graph() -> Node:
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

    grandparent = Node(node_id="grandparent")
    parent = Node(node_id="parent")
    child1 = Node(node_id="child1")
    grandchild1 = Node(node_id="grandchild1")
    child2 = Node(node_id="child2")
    parent2 = Node(node_id="parent2")
    child3 = Node(node_id="child3")
    child4 = Node(node_id="child4")
    parent3 = Node(node_id="parent3")

    grandparent_to_parent = NodeLink(
        parent_id=grandparent.node_id,
        child_id=parent.node_id,
        label="grandparent_to_parent",
    )
    parent_to_child1 = NodeLink(
        parent_id=parent.node_id,
        child_id=child1.node_id,
        label="parent_to_child1",
    )
    child1_to_grandchild1 = NodeLink(
        parent_id=child1.node_id,
        child_id=grandchild1.node_id,
        label="child1_to_grandchild1",
    )
    parent_to_child2 = NodeLink(
        parent_id=parent.node_id,
        child_id=child2.node_id,
        label="parent_to_child2",
    )
    grandparent_to_parent2 = NodeLink(
        parent_id=grandparent.node_id,
        child_id=parent2.node_id,
        label="grandparent_to_parent2",
    )
    parent2_to_child3 = NodeLink(
        parent_id=parent2.node_id,
        child_id=child3.node_id,
        label="parent2_to_child3",
    )
    parent2_to_child4 = NodeLink(
        parent_id=parent2.node_id,
        child_id=child4.node_id,
        label="parent2_to_child4",
    )
    grandparent_to_parent3 = NodeLink(
        parent_id=grandparent.node_id,
        child_id=parent3.node_id,
        label="grandparent_to_parent3",
    )

    await write(
        [
            grandparent,
            parent,
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

    return grandparent
