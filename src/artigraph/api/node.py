from collections.abc import Collection
from dataclasses import fields
from typing import Any, Literal, Sequence, TypeGuard, TypeVar, overload

from sqlalchemy import Row, delete, join, select
from sqlalchemy.orm import aliased

from artigraph.db import current_session
from artigraph.orm.node import Node

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


async def read_node(
    node_id: int, node_type: type[N] = Node, *, allow_none: bool = False
) -> N | None:
    """Read a node by its ID."""
    cmd = select(node_type).where(node_type.node_id == node_id)
    async with current_session() as session:
        result = await session.execute(cmd)
        return result.scalar_one_or_none() if allow_none else result.scalar_one()


async def node_exists(node_id: int, node_type: type[Node] = Node) -> bool:
    """Check if a node exists."""
    cmd = select(node_type.node_id).where(node_type.node_id == node_id)
    async with current_session() as session:
        result = await session.execute(cmd)
        return bool(result.one_or_none())


async def delete_nodes(node_ids: Sequence[int]) -> None:
    """Delete nodes."""
    async with current_session() as session:
        cmd = delete(Node).where(Node.node_id.in_(node_ids))
        await session.execute(cmd)
        await session.commit()


async def create_nodes(
    nodes: Collection[Node], refresh_attributes: Sequence[str]
) -> Collection[Node]:
    """Create nodes and, if given, refresh their attributes."""
    async with current_session() as session:
        session.add_all(nodes)
        await session.commit()
        if refresh_attributes:
            # We can't do this in asyncio.gather() because of issues with concurrent connections:
            # https://docs.sqlalchemy.org/en/20/errors.html#illegalstatechangeerror-and-concurrency-exceptions
            for n in nodes:
                await session.refresh(n, refresh_attributes)
    return nodes


async def create_parent_child_relationships(
    parent_child_pairs: Sequence[tuple[Node | None, Node]]
) -> None:
    """Create parent-to-child links between nodes."""
    async with current_session() as session:
        for parent, child in parent_child_pairs:
            child.node_parent_id = None if parent is None else parent.node_id
            session.add(child)
        await session.commit()


async def read_children(node_id: int, *node_types: type[N]) -> Sequence[N]:
    """Read the direct children of a node."""
    cmd = select(Node).where(Node.node_parent_id == node_id)
    if node_types:
        cmd = cmd.where(Node.node_type.in_([n.polymorphic_identity for n in node_types]))
    async with current_session() as session:
        result = await session.execute(cmd)
        children = result.scalars().all()
    # we know we've filtered appropriately, so we can ignore the type check
    return children  # type: ignore


async def read_parent(node_id: int, node_type: type[N] = Node) -> N | None:
    """Read the direct parent of a node."""
    async with current_session() as session:
        node_cmd = select(node_type).where(node_type.node_id == node_id)
        result = await session.execute(node_cmd)
        node = result.scalar_one()
        parent_cmd = select(node_type).where(node_type.node_id == node.node_parent_id)
        result = await session.execute(parent_cmd)
        return result.scalar_one_or_none()


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
    descendants_cmd = (
        select(Node.__table__)
        .select_from(join(Node, node_cte, Node.node_id == node_cte.c.descendant_id))
        .where(node_cte.c.descendant_id != node_id)  # Exclude the root node itself
    )

    if node_types:
        descendants_cmd = descendants_cmd.where(
            Node.node_type.in_([n.polymorphic_identity for n in node_types])
        )

    async with current_session() as session:
        result = await session.execute(descendants_cmd)
        descendants = result.all()

    return load_nodes_from_rows(descendants)


async def read_ancestors(node_id: int, *node_types: type[N]) -> Sequence[N]:
    """Read all ancestors of this node."""

    # Create a CTE to get the ancestors recursively
    node_cte = (
        select(Node.node_id.label("ancestor_id"), Node.node_parent_id)
        .where(Node.node_id == node_id)
        .cte(name="ancestors", recursive=True)
    )

    # Recursive case: select the parents of the current nodes
    parent_node = aliased(Node)
    node_cte = node_cte.union_all(
        select(parent_node.node_id, parent_node.node_parent_id).where(
            parent_node.node_id == node_cte.c.node_parent_id
        )
    )

    # Join the CTE with the actual Node table to get the ancestors
    ancestors_cmd = (
        select(Node.__table__)
        .select_from(join(Node, node_cte, Node.node_id == node_cte.c.ancestor_id))
        .where(node_cte.c.ancestor_id != node_id)  # Exclude the root node itself
    )

    if node_types:
        ancestors_cmd = ancestors_cmd.where(
            Node.node_type.in_([n.polymorphic_identity for n in node_types])
        )

    async with current_session() as session:
        result = await session.execute(ancestors_cmd)
        ancestors = result.all()

    return load_nodes_from_rows(ancestors)


def load_nodes_from_rows(rows: Sequence[Row[Any]]) -> Sequence[Any]:
    """Load the appropriate Node instances given a sequence of SQLAlchemy rows."""
    nodes: list[Any] = []
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
