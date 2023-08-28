from __future__ import annotations

from artigraph.db import current_session
from artigraph.orm.link import OrmNodeLink
from artigraph.orm.node import OrmNode


class ThingOne(OrmNode):
    polymorphic_identity = "thing_one"
    __mapper_args__ = {"polymorphic_identity": polymorphic_identity}  # noqa: RUF012


class ThingTwo(OrmNode):
    polymorphic_identity = "thing_two"
    __mapper_args__ = {"polymorphic_identity": polymorphic_identity}  # noqa: RUF012


async def test_read_write_


# async def test_read_node_type_any_of():
#     graph = await create_graph()
#     nodes = await orm_read(OrmNode, NodeFilter(node_type=NodeTypeFilter(type=[ThingOne])))
#     assert {n.node_id for n in nodes} == {
#         n.node_id for n in graph.get_all_nodes() if isinstance(n, ThingOne)
#     }


# async def test_read_node_type_none_of():
#     graph = await create_graph()
#     nodes = await read(NodeFilter(node_type=NodeTypeFilter(not_type=[ThingOne])))
#     assert {n.node_id for n in nodes} == {
#         n.node_id for n in graph.get_all_nodes() if not isinstance(n, ThingOne)
#     }


# async def test_delete_node():
#     graph = await create_graph()
#     root = graph.get_root()
#     node_filter = NodeFilter(node_id=ValueFilter(eq=root.node_id))
#     await delete(node_filter)
#     assert not await exists(node_filter)


# async def test_delete_node_also_deletes_links():
#     graph = await create_graph()
#     root = graph.get_root()
#     node_filter = NodeFilter(node_id=root.node_id)
#     node_link_filter = NodeLinkFilter(parent=node_filter) | NodeLinkFilter(child=node_filter)
#     assert await read_links(node_link_filter)
#     await delete(node_filter)
#     assert not await read_links(node_link_filter)


# async def test_recursive_delete_node():
#     graph = await create_graph()
#     root = graph.get_root()
#     node_filter = NodeFilter(descendant_of=root.node_id)
#     await delete(node_filter)
#     assert not await exists(node_filter)


# async def test_read_direct_children():
#     """Test reading the direct children of a node."""
#     graph = await create_graph()
#     root = graph.get_root()
#     node_filter = NodeFilter(child_of=root.node_id)
#     children = await read(node_filter)
#     assert {n.node_id for n in children} == {n.node_id for n in graph.get_children(root.node_id)}


# async def test_read_decendant_nodes():
#     """Test reading the recursive children of a node."""
#     graph = await create_graph()
#     root = graph.get_root()
#     node_filter = NodeFilter(descendant_of=root.node_id)
#     children = await read(node_filter)
#     expected_descendant_ids = {n.node_id for n in graph.get_all_nodes()} - {root.node_id}
#     assert {n.node_id for n in children} == expected_descendant_ids


# async def test_create_parent_child_relationships():
#     """Test creating parent-to-child relationships between nodes."""
#     async with new_session(expire_on_commit=False):
#         grandparent = await create_node()
#         parent = await create_node()
#         child = await create_node()
#         grandchild = await create_node()

#         node_links = [
#             OrmNodeLink(parent_id=grandparent.node_id, child_id=parent.node_id),
#             OrmNodeLink(parent_id=parent.node_id, child_id=child.node_id),
#             OrmNodeLink(parent_id=child.node_id, child_id=grandchild.node_id),
#         ]
#         await write_links(node_links)
#         db_node_links = await read_links(
#             NodeLinkFilter(child=[parent.node_id, child.node_id, grandchild.node_id])
#         )
#         assert {nl.id for nl in db_node_links} == {nl.link_id for nl in node_links}


# async def test_read_ancestor_nodes():
#     """Test reading the ancestor nodes of a node."""
#     async with new_session(expire_on_commit=False):
#         grandparent = await create_node()
#         parent = await create_node()
#         child = await create_node()
#         grandchild = await create_node()
#         await write_links(
#             [
#                 OrmNodeLink(parent_id=grandparent.node_id, child_id=parent.node_id),
#                 OrmNodeLink(parent_id=parent.node_id, child_id=child.node_id),
#                 OrmNodeLink(parent_id=child.node_id, child_id=grandchild.node_id),
#             ]
#         )
#         actual_node_ids = {
#             n.node_id for n in await read(NodeFilter(ancestor_of=grandchild.node_id))
#         }
#         assert actual_node_ids == {
#             grandparent.node_id,
#             parent.node_id,
#             child.node_id,
#         }


# async def test_read_parent_node():
#     parent = await create_node()
#     child = await create_node()
#     await write_links([OrmNodeLink(parent_id=parent.node_id, child_id=child.node_id)])
#     node_filter = NodeFilter(parent_of=child.node_id)
#     assert (await read_one(node_filter)).node_id == parent.node_id


async def create_graph() -> GraphNode:
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
    graph = GraphNode(None)
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


class GraphNode:
    """Simple graph for testing purposes."""

    def __init__(self, parent: OrmNode | None):
        self.parent = parent
        self.children: list[GraphNode] = []

    def add_child(self, node_type: type[OrmNode]):
        node = node_type()
        graph = GraphNode(node)
        self.children.append(graph)
        return graph

    def get_root(self) -> OrmNode:
        return self.parent if self.parent is not None else self.children[0].get_root()

    def get_all_nodes(self) -> list[OrmNode]:
        nodes = [] if self.parent is None else [self.parent]
        for child in self.children:
            nodes.extend(child.get_all_nodes())
        return nodes

    def get_all_nodes_by_parent_id(self) -> dict[str | None, list[OrmNode]]:
        parent_id = self.parent.node_id if self.parent else None
        nodes_by_parent_id = {parent_id: [c.parent for c in self.children if c.parent]}
        for c in self.children:
            nodes_by_parent_id.update(c.get_all_nodes_by_parent_id())
        return nodes_by_parent_id

    def get_children(self, key: str | None) -> list[OrmNode]:
        return self.get_all_nodes_by_parent_id()[key]

    async def create(self) -> None:
        async with current_session() as session:
            if self.parent is not None:
                session.add(self.parent)
                for c in self.children:
                    if c.parent:
                        session.add(
                            OrmNodeLink(
                                parent_id=self.parent.node_id,
                                child_id=c.parent.node_id,
                            )
                        )

            for c in self.children:
                await c.create()
            await session.commit()

    def __repr__(self) -> str:
        return f"Graph(parent={self.parent}, children={self.children})"
