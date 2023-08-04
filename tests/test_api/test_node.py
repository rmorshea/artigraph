from artigraph.api.node import (
    create_parent_child_relationships,
    group_nodes_by_parent_id,
    read_children,
    read_descendants,
    read_node,
)
from artigraph.db import current_session, session_context
from artigraph.orm.node import Node


class ThingOne(Node):
    polymorphic_identity = "thing_one"
    __mapper_args__ = {"polymorphic_identity": polymorphic_identity}  # noqa: RUF012


class ThingTwo(Node):
    polymorphic_identity = "thing_two"
    __mapper_args__ = {"polymorphic_identity": polymorphic_identity}  # noqa: RUF012


async def test_read_direct_children():
    """Test reading the direct children of a node."""
    graph = await create_graph()
    root = graph.get_root()
    children = await read_children(root.node_id)
    assert {n.node_id for n in children} == {n.node_id for n in graph.get_children(root.node_id)}


async def test_read_direct_children_with_node_types():
    """Test reading the direct children of a node with node types."""
    graph = await create_graph()
    root = graph.get_root()
    children = await read_children(root.node_id, ThingOne)
    expected_ids = {n.node_id for n in graph.get_children(root.node_id) if isinstance(n, ThingOne)}
    assert {n.node_id for n in children} == expected_ids


async def test_read_recursive_children():
    """Test reading the recursive children of a node."""
    graph = await create_graph()
    root = graph.get_root()
    children = await read_descendants(root.node_id)
    expected_descendant_ids = {n.node_id for n in graph.get_all_nodes()} - {root.node_id}
    assert {n.node_id for n in children} == expected_descendant_ids


async def test_read_recursive_children_with_node_type():
    """Test reading the recursive children of a node with node types."""
    graph = await create_graph()
    root = graph.get_root()
    children = await read_descendants(root.node_id, ThingOne)
    all_span_ids = {n.node_id for n in graph.get_all_nodes() if isinstance(n, ThingOne)}
    expected_descendant_ids = all_span_ids - {root.node_id}
    assert {n.node_id for n in children} == expected_descendant_ids


async def test_create_parent_child_relationships():
    """Test creating parent-to-child relationships between nodes."""
    async with session_context(expire_on_commit=False):
        grandparent = await create_node()
        parent = await create_node(grandparent)
        child = await create_node(parent)
        await create_parent_child_relationships([(grandparent, parent), (parent, child)])

        db_parent = await read_node(parent.node_id)
        db_child = await read_node(child.node_id)

        assert db_parent.node_parent_id == grandparent.node_id
        assert db_child.node_parent_id == parent.node_id


async def create_node(parent=None):
    node = Node(node_parent_id=parent.node_id if parent else None)

    async with current_session() as session:
        session.add(node)
        await session.commit()
        await session.refresh(node)

    return node


async def create_graph() -> "Graph":
    """Create a simple tree of nodes.

    The tree looks like this:

    ThingOne
    ├── ThingOne
    │   ├── ThingTwo
    │   │   └── ThingTwo
    │   └── ThingOne
    └── ThingOne
        ├── ThingOne
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

    await graph.create()

    return graph


class Graph:
    """Simple graph for testing purposes."""

    def __init__(self, parent: Node | None):
        self.parent = parent
        self.children: list[Graph] = []

    def add_child(self, node_type: type[Node]):
        node = node_type(None)
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
                    c.parent.node_parent_id = self.parent.node_id
            for c in self.children:
                await c.create()
