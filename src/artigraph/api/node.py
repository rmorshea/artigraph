from dataclasses import fields
from typing import Any, Iterator, Literal, Sequence, TypeGuard, TypeVar, overload

from sqlalchemy import Row, delete, join, select
from sqlalchemy.orm import aliased

from artigraph.db import current_session
from artigraph.orm.node import Node
from artigraph.utils import syncable

T = TypeVar("T")
N = TypeVar("N", bound=Node)


def group_nodes_by_parent_id(nodes: Sequence[N]) -> dict[int | None, list[N]]:
    """Group nodes by their parent ID."""
    grouped_nodes: dict[int | None, list[N]] = {}
    for node in nodes:
        grouped_nodes.setdefault(node.node_parent_id, []).append(node)
    return grouped_nodes


def is_node_type(node: Node, node_type: type[N]) -> TypeGuard[N]:
    """Check if a node is of a given type."""
    return node.node_type == node_type.polymorphic_identity


@overload
async def read_node(
    node_id: int,
    node_type: type[N] = Node,
    *,
    allow_none: Literal[True],
) -> N | None:
    ...


@overload
async def read_node(
    node_id: int,
    node_type: type[N] = Node,
    *,
    allow_none: Literal[False] = ...,
) -> N:
    ...


async def read_node(node_id: int, node_type: type[N] = Node, *, allow_none: bool = False) -> N:
    """Read a node by its ID."""
    stmt = select(node_type).where(node_type.node_id == node_id)
    async with current_session() as session:
        result = await session.execute(stmt)
        return result.scalar_one_or_none() if allow_none else result.scalar_one()


read_nodes = syncable(read_node)


@syncable
async def node_exists(node_id: int, node_type: type[Node] = Node) -> bool:
    """Check if a node exists."""
    stmt = select(node_type.node_id).where(node_type.node_id == node_id)
    async with current_session() as session:
        result = await session.execute(stmt)
        return bool(result.one_or_none())


@syncable
async def delete_nodes(node_ids: Sequence[int]) -> None:
    """Delete nodes."""
    async with current_session() as session:
        stmt = delete(Node).where(Node.node_id.in_(node_ids))
        await session.execute(stmt)
        await session.commit()


@syncable
async def create_parent_child_relationships(
    parent_child_pairs: Sequence[tuple[Node, Node]]
) -> None:
    """Create parent-to-child links between nodes."""
    async with current_session() as session:
        for parent, child in parent_child_pairs:
            child.node_parent_id = parent.node_id
            session.add(child)
        await session.commit()


@syncable
async def read_children(node_id: int, *node_types: type[N]) -> Sequence[N]:
    """Read the direct children of a node."""
    stmt = select(Node).where(Node.node_parent_id == node_id)
    if node_types:
        stmt = stmt.where(Node.node_type.in_([n.polymorphic_identity for n in node_types]))
    async with current_session() as session:
        result = await session.execute(stmt)
        children = result.scalars().all()
    # we know we've filtered appropriately, so we can ignore the type check
    return children  # type: ignore


@syncable
async def read_descendants(node_id: int, *node_types: type[N]) -> Sequence[N]:
    """Read all descendants of this node."""

    # Create a CTE to get the descendants recursively
    node_cte = (
        select(Node.node_id.label("descendant_id"), Node.node_parent_id)
        .where(Node.node_id == node_id)
        .cte(name="descendants", recursive=True)
    )

    # Recursive case: select the children of the current nodes
    parent_node = aliased(Node)
    node_cte = node_cte.union_all(
        select(parent_node.node_id, parent_node.node_parent_id).where(
            parent_node.node_parent_id == node_cte.c.descendant_id
        )
    )

    # Join the CTE with the actual Node table to get the descendants
    descendants_query = (
        select(Node.__table__)
        .select_from(join(Node, node_cte, Node.node_id == node_cte.c.descendant_id))
        .where(node_cte.c.descendant_id != node_id)  # Exclude the root node itself
    )

    if node_types:
        descendants_query = descendants_query.where(
            Node.node_type.in_([n.polymorphic_identity for n in node_types])
        )

    async with current_session() as session:
        result = await session.execute(descendants_query)
        descendants = result.all()

    return _nodes_from_rows(descendants)


def _nodes_from_rows(rows: Row[Any]) -> Iterator[Node]:
    nodes: list[Node] = []
    for r in rows:
        node_type = Node.polymorphic_identity_mapping[r.node_type]
        kwargs: dict[str, Any] = {}
        attrs: dict[str, Any] = {}
        for f in fields(node_type):
            if f.init:
                kwargs[f.name] = getattr(r, f.name)
            else:
                attrs[f.name] = getattr(r, f.name)
        node_obj = node_type(**kwargs)
        node_obj.__dict__.update(attrs)
        nodes.append(node_obj)
    return nodes
