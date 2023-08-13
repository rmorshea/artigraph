from __future__ import annotations

from artigraph.api.filter import NodeFilter, NodeRelationshipFilter, NodeTypeFilter, ValueFilter
from artigraph.api.node import (
    delete_nodes,
    group_nodes_by_parent_id,
    new_node,
    read_node,
    read_nodes,
    read_nodes_exist,
    write_node,
    write_parent_child_relationships,
)
from artigraph.db import current_session, new_session
from artigraph.orm.node import Node


class ThingOne(Node):
    polymorphic_identity = "thing_one"
    __mapper_args__ = {"polymorphic_identity": polymorphic_identity}  # noqa: RUF012


class ThingTwo(Node):
    polymorphic_identity = "thing_two"
    __mapper_args__ = {"polymorphic_identity": polymorphic_identity}  # noqa: RUF012


async def test_read_node_type_any_of():
    graph = await create_graph()
    nodes = await read_nodes(NodeFilter(node_type=NodeTypeFilter(type=[ThingOne])))
    assert {n.node_id for n in nodes} == {
        n.node_id for n in graph.get_all_nodes() if isinstance(n, ThingOne)
    }


async def test_read_node_type_none_of():
    graph = await create_graph()
    nodes = await read_nodes(NodeFilter(node_type=NodeTypeFilter(not_type=[ThingOne])))
    assert {n.node_id for n in nodes} == {
        n.node_id for n in graph.get_all_nodes() if not isinstance(n, ThingOne)
    }


async def test_delete_node():
    graph = await create_graph()
    root = graph.get_root()
    node_filter = NodeFilter(node_id=ValueFilter(eq=root.node_id))
    await delete_nodes(node_filter)
    assert not await read_nodes_exist(node_filter)


async def test_recursive_delete_node():
    graph = await create_graph()
    root = graph.get_root()
    node_filter = NodeFilter(relationship=NodeRelationshipFilter(descendant_of=root.node_id))
    await delete_nodes(node_filter)
    assert not await read_nodes_exist(node_filter)


async def test_read_direct_children():
    """Test reading the direct children of a node."""
    graph = await create_graph()
    root = graph.get_root()
    node_filter = NodeFilter(relationship=NodeRelationshipFilter(child_of=root.node_id))
    children = await read_nodes(node_filter)
    assert {n.node_id for n in children} == {n.node_id for n in graph.get_children(root.node_id)}


async def test_read_recursive_children():
    """Test reading the recursive children of a node."""
    graph = await create_graph()
    root = graph.get_root()
    node_filter = NodeFilter(relationship=NodeRelationshipFilter(descendant_of=root.node_id))
    children = await read_nodes(node_filter)
    expected_descendant_ids = {n.node_id for n in graph.get_all_nodes()} - {root.node_id}
    assert {n.node_id for n in children} == expected_descendant_ids


async def test_create_parent_child_relationships():
    """Test creating parent-to-child relationships between nodes."""
    async with new_session(expire_on_commit=False):
        grandparent = await create_node()
        parent = await create_node(grandparent)
        child = await create_node(parent)
        grandchild = await create_node(child)
        await write_parent_child_relationships(
            [
                (grandparent.node_id, parent.node_id),
                (parent.node_id, child.node_id),
                (child.node_id, grandchild.node_id),
            ]
        )

        db_parent = await read_node(NodeFilter(node_id=ValueFilter(eq=parent.node_id)))
        db_child = await read_node(NodeFilter(node_id=ValueFilter(eq=child.node_id)))
        db_grandchild = await read_node(NodeFilter(node_id=ValueFilter(eq=grandchild.node_id)))

        assert db_parent.node_parent_id == grandparent.node_id
        assert db_child.node_parent_id == parent.node_id
        assert db_grandchild.node_parent_id == child.node_id

        actual_node_ids = {
            n.node_id
            for n in await read_nodes(
                NodeFilter(relationship=NodeRelationshipFilter(descendant_of=grandparent.node_id))
            )
        }
        assert actual_node_ids == {
            parent.node_id,
            child.node_id,
            grandchild.node_id,
        }


async def test_read_ancestor_nodes():
    """Test reading the ancestor nodes of a node."""
    async with new_session(expire_on_commit=False):
        grandparent = await create_node()
        parent = await create_node(grandparent)
        child = await create_node(parent)
        grandchild = await create_node(child)
        await write_parent_child_relationships(
            [
                (grandparent.node_id, parent.node_id),
                (parent.node_id, child.node_id),
                (child.node_id, grandchild.node_id),
            ]
        )
        actual_node_ids = {
            n.node_id
            for n in await read_nodes(
                NodeFilter(relationship=NodeRelationshipFilter(ancestor_of=grandchild.node_id))
            )
        }
        assert actual_node_ids == {
            grandparent.node_id,
            parent.node_id,
            child.node_id,
        }


async def test_read_parent_node():
    async with new_session(expire_on_commit=False):
        parent = await create_node()
        child = await create_node(parent)
    node_filter = NodeFilter(relationship=NodeRelationshipFilter(parent_of=child.node_id))
    assert (await read_node(node_filter)).node_id == parent.node_id


async def create_node(parent=None):
    node = Node(node_parent_id=parent.node_id if parent else None)

    async with current_session() as session:
        session.add(node)
        await session.commit()
        await session.refresh(node)

    return node


async def create_graph() -> Graph:
    """Create a simple tree of nodes.

    The tree looks like this:

    ThingOne
    ├── ThingOne
    │   ├── ThingTwo
    │   │   └── ThingTwo
    │   └── ThingOne
    ├── ThingOne
    │  ├── ThingOne
    │   └── ThingTwo
    └── ThingTwo
    """
    graph = Graph(None)
    root = graph.add_child(ThingOne)
    child1 = root.add_child(ThingOne)
    grandchild1 = child1.add_child(ThingTwo)
    grandchild1.add_child(ThingTwo)
    child2 = root.add_child(ThingOne)
    child2.add_child(ThingOne)
    child2.add_child(ThingTwo)
    root.add_child(ThingTwo)

    await graph.create()

    return graph


class Graph:
    """Simple graph for testing purposes."""

    def __init__(self, parent: Node | None):
        self.parent = parent
        self.children: list[Graph] = []

    def add_child(self, node_type: type[Node]):
        node = node_type(node_parent_id=None)
        graph = Graph(node)
        self.children.append(graph)
        return graph

    def get_root(self) -> Node:
        return self.parent if self.parent is not None else self.children[0].get_root()

    def get_all_nodes(self) -> list[Node]:
        nodes = [] if self.parent is None else [self.parent]
        for child in self.children:
            nodes.extend(child.get_all_nodes())
        return nodes

    def get_all_nodes_by_parent_id(self) -> dict[int | None, list[Node]]:
        return group_nodes_by_parent_id(self.get_all_nodes())

    def get_children(self, key: int | None) -> list[Node]:
        return self.get_all_nodes_by_parent_id()[key]

    async def create(self) -> None:
        async with current_session() as session:
            if self.parent is not None:
                session.add(self.parent)
                await session.commit()
                await session.refresh(self.parent)
                for c in self.children:
                    if c.parent:
                        c.parent.node_parent_id = self.parent.node_id
            for c in self.children:
                await c.create()

    def __repr__(self) -> str:
        return f"Graph(parent={self.parent}, children={self.children})"


async def test_new_node_with_node_id():
    """Test creating a new node with a node_id."""
    node = new_node(node_id=1)
    await write_node(node)
    await read_node(NodeFilter(node_id=1))
