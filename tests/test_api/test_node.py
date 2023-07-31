import asyncio

from artigraph.api.node import (
    create_metadata,
    group_nodes_by_parent_id,
    read_direct_children,
    read_metadata,
    read_recursive_children,
)
from artigraph.db import current_session
from artigraph.orm.node import Node


async def test_create_and_read_metadata():
    """Test creating and reading metadata for a node."""
    node = await create_node()
    metadata = {"foo": "bar", "baz": "qux"}
    await create_metadata(node, metadata)
    assert await read_metadata(node) == metadata


async def test_read_direct_children():
    """Test reading the direct children of a node."""
    graph = await create_graph()
    root = graph.get_root()
    children = await read_direct_children(root)
    assert {n.id for n in children} == {n.id for n in graph.get_children(root.id)}


async def test_read_direct_children_with_node_types():
    """Test reading the direct children of a node with node types."""
    graph = await create_graph()
    root = graph.get_root()
    children = await read_direct_children(root, node_types=["first"])
    assert {n.id for n in children} == {
        n.id for n in graph.get_children(root.id) if n.type == "first"
    }


async def test_read_recursive_children():
    """Test reading the recursive children of a node."""
    graph = await create_graph()
    root = graph.get_root()
    children = await read_recursive_children(root)
    assert {n.id for n in children} == {n.id for n in graph.get_all_nodes()}


async def create_node(parent=None):
    node = Node(parent_id=parent.id if parent else None)
    node.type = "simple"

    async with current_session() as session:
        session.add(node)
        await session.commit()
        await session.refresh(node)

    return node


async def create_graph() -> "Graph":
    """Create a simple tree of nodes.

    The tree looks like this:

    root(type=run)
    ├── child1(type=run)
    │   ├── grandchild1(type=storage_artifact)
    │   │   └── greatgrandchild1(type=storage_artifact)
    │   └── grandchild2(type=run)
    └── child2(type=run)
        ├── grandchild3(type=run)
        └── grandchild4(type=storage_artifact)
    """
    graph = Graph(None)
    root = graph.add_child("run")
    child1 = root.add_child("run")
    grandchild1 = child1.add_child("storage_artifact")
    grandchild1.add_child("storage_artifact")
    child2 = root.add_child("run")
    child2.add_child("run")
    child2.add_child("storage_artifact")

    await graph.create()

    return graph


class Graph:
    """Simple graph for testing purposes."""

    def __init__(self, parent: Node | None):
        self.parent = parent
        self.children: list[Graph] = []

    def add_child(self, node_type: str):
        node = Node(None)
        node.type = node_type
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
                    c.parent.parent_id = self.parent.id
            for c in self.children:
                await c.create()
