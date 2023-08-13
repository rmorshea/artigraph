from __future__ import annotations

from collections.abc import Collection
from dataclasses import fields
from typing import (
    Any,
    Callable,
    Iterable,
    Sequence,
    TypeVar,
    overload,
)

from sqlalchemy import case, delete, select, update
from typing_extensions import ParamSpec

from artigraph.api.filter import Filter, NodeFilter
from artigraph.db import current_session
from artigraph.orm.node import NODE_TYPE_BY_POLYMORPHIC_IDENTITY, Node

P = ParamSpec("P")
N = TypeVar("N", bound=Node)


def group_nodes_by_parent_id(nodes: Sequence[N]) -> dict[int | None, list[N]]:
    """Group nodes by their parent ID."""
    grouped_nodes: dict[int | None, list[N]] = {}
    for node in nodes:
        grouped_nodes.setdefault(node.node_parent_id, []).append(node)
    return grouped_nodes


def new_node(node_type: Callable[P, N] = Node, *args: P.args, **kwargs: P.kwargs) -> N:
    """Create a new node."""
    kwargs.setdefault("node_parent_id", None)
    node_id = kwargs.pop("node_id", None)
    node = node_type(*args, **kwargs)
    if node_id is not None:
        node.node_id = node_id  # type: ignore
    return node


async def read_nodes_exist(node_filter: NodeFilter[Any] | Filter) -> bool:
    """Check if nodes exist."""
    return bool(await read_nodes(node_filter))


async def read_node(node_filter: NodeFilter[N] | Filter) -> N:
    """Read a node that matches the given filter."""
    node = await read_node_or_none(node_filter)
    if node is None:
        msg = f"No node found for filter {node_filter}"
        raise ValueError(msg)
    return node


async def read_node_or_none(node_filter: NodeFilter[N] | Filter) -> N | None:
    """Read a node that matches the given filter or None if no node is found."""
    async with current_session() as session:
        result = await session.execute(node_filter.apply(select(Node.__table__)))
        return load_node_from_row(result.one_or_none())  # type: ignore


async def read_nodes(node_filter: NodeFilter[N] | Filter) -> Sequence[N]:
    """Read nodes that match the given filter."""
    cmd = node_filter.apply(select(Node.__table__))
    async with current_session() as session:
        result = await session.execute(cmd)
        return load_nodes_from_rows(result.all())


async def delete_nodes(node_filter: NodeFilter[Any] | Filter) -> None:
    """Delete nodes matching the given filter."""

    async with current_session() as session:
        # node_ids_cmd: Select[tuple[int]] = node_filter.apply(select(Node.node_id))
        # node_ids = (await session.execute(node_ids_cmd)).scalars().all()
        delete_cmd = node_filter.apply(delete(Node))
        await session.execute(delete_cmd)
        await session.commit()


async def write_node(node: Node, *, refresh_attributes: Sequence[str] = ()) -> Node:
    """Create a node."""
    return (await write_nodes([node], refresh_attributes=refresh_attributes))[0]


async def write_nodes(
    nodes: Collection[Node], *, refresh_attributes: Sequence[str]
) -> Sequence[Node]:
    """Create nodes and, if given, refresh their attributes."""
    async with current_session() as session:
        session.add_all(nodes)
        await session.commit()
        if refresh_attributes:
            # We can't do this in asyncio.gather() because of issues with concurrent connections:
            # https://docs.sqlalchemy.org/en/20/errors.html#illegalstatechangeerror-and-concurrency-exceptions
            for n in nodes:
                await session.refresh(n, refresh_attributes or None)
    return tuple(nodes)


async def write_parent_child_relationships(
    parent_child_id_pairs: Iterable[tuple[int | None, int]]
) -> None:
    """Create parent-to-child links between nodes.

    Updates the existing child node's node_parent_id.
    """

    # Build the CASE statement for the update query
    parent_id_conditions = [
        (Node.node_id == child_id, parent_id) for parent_id, child_id in parent_child_id_pairs
    ]

    # Build the update query
    cmd = (
        update(Node)
        .where(Node.node_id.in_([child_id for _, child_id in parent_child_id_pairs]))
        .values(node_parent_id=case(*parent_id_conditions))
    )

    async with current_session() as session:
        await session.execute(cmd)
        await session.commit()


def load_nodes_from_rows(rows: Sequence[Any]) -> Sequence[Any]:
    """Load the appropriate Node instances given a sequence of SQLAlchemy rows."""
    return list(map(load_node_from_row, rows))


@overload
def load_node_from_row(row: None) -> None:
    ...


@overload
def load_node_from_row(row: Any) -> Node:
    ...


def load_node_from_row(row: Any | None) -> Node | None:
    """Load the appropriate Node instance given a SQLAlchemy row."""
    if row is None:
        return None
    node_type = NODE_TYPE_BY_POLYMORPHIC_IDENTITY[row.node_type]
    kwargs: dict[str, Any] = {}
    attrs: dict[str, Any] = {}
    for f in fields(node_type):
        if f.init:
            kwargs[f.name] = getattr(row, f.name)
        else:
            attrs[f.name] = getattr(row, f.name)
    node_obj = node_type(**kwargs)
    node_obj.__dict__.update(attrs)
    return node_obj
