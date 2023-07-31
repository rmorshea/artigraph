from artigraph.api.node import create_metadata, group_nodes_by_parent_id, read_metadata
from artigraph.db import current_session
from artigraph.orm.node import Node


async def test_create_metadata():
    """Test creating metadata for a node."""
    node = await create_node()
    metadata = {"foo": "bar", "baz": "qux"}
    await create_metadata(node, metadata)
    assert await read_metadata(node) == metadata


async def create_node(parent=None):
    node = Node(parent_id=parent.id if parent else None)
    node.type = "simple"

    async with current_session() as session:
        session.add(node)
        await session.commit()
        await session.refresh(node)

    return node


async def create_simple_graph() -> dict[int | None, list[Node]]:
    """Create a simple tree of nodes.

    The tree looks like this:

    root
    ├── child1
    │   ├── grandchild1
    │   │   └── greatgrandchild1
    │   └── grandchild2
    └── child2
        ├── grandchild3
        └── grandchild4
    """

    def create_node(parent=None):
        node = Node(parent_id=parent.id if parent else None)
        node.type = "simple"
        return node

    root = create_node()
    child1 = create_node(root)
    grandchild1 = create_node(child1)
    greatgrandchild1 = create_node(grandchild1)
    grandchild2 = create_node(child1)
    child2 = create_node(root)
    grandchild3 = create_node(child2)
    grandchild4 = create_node(child2)

    nodes = [
        root,
        child1,
        grandchild1,
        greatgrandchild1,
        grandchild2,
        child2,
        grandchild3,
        grandchild4,
    ]

    async with current_session() as session:
        session.add_all(nodes)
        await session.commit()

    return group_nodes_by_parent_id(nodes)
