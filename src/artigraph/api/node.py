from __future__ import annotations

from collections.abc import Collection
from dataclasses import fields
from typing import (
    Any,
    Sequence,
    TypeVar,
    overload,
)

from sqlalchemy import delete, select
from typing_extensions import ParamSpec

from artigraph.api.filter import Filter, NodeFilter, NodeLinkFilter
from artigraph.db import current_session
from artigraph.orm.link import NodeLink
from artigraph.orm.node import NODE_TYPE_BY_POLYMORPHIC_IDENTITY, Node

P = ParamSpec("P")
N = TypeVar("N", bound=Node)


async def read_nodes_exist(node_filter: NodeFilter[Any] | Filter) -> bool:
    """Check if nodes exist."""
    return bool(await read_nodes(node_filter))


async def read_node(node_filter: NodeFilter[N] | Filter) -> N:
    """Read a node that matches the given filter."""
    node = await read_node_or_none(node_filter)
    if node is None:
        msg = f"No node found for filter {node_filter!r}"
        raise ValueError(msg)
    return node


async def read_node_or_none(node_filter: NodeFilter[N] | Filter) -> N | None:
    """Read a node that matches the given filter or None if no node is found."""
    async with current_session() as session:
        result = await session.execute(select(Node.__table__).where(node_filter.create()))
        return load_node_from_row(result.one_or_none())  # type: ignore


async def read_nodes(node_filter: NodeFilter[N] | Filter) -> Sequence[N]:
    """Read nodes that match the given filter."""
    cmd = select(Node.__table__).where(node_filter.create())
    async with current_session() as session:
        result = await session.execute(cmd)
        return load_nodes_from_rows(result.all())


async def delete_nodes(node_filter: NodeFilter[Any] | Filter) -> None:
    """Delete nodes matching the given filter."""
    async with current_session() as session:
        node_ids = select(Node.node_id).where(node_filter.create())
        delete_nodes_cmd = delete(Node).where(Node.node_id.in_(node_ids))
        node_link_filter = NodeLinkFilter(parent=node_ids) | NodeLinkFilter(child=node_ids)
        delete_links_cmd = delete(NodeLink).where(node_link_filter.create())
        await session.execute(delete_nodes_cmd)
        await session.execute(delete_links_cmd)
        await session.commit()
        await delete_node_links(node_link_filter)


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


async def read_node_link(node_link_filter: NodeLinkFilter[Any] | Filter) -> NodeLink:
    """Read a parent-to-child link between nodes."""
    node_link = await read_node_link_or_none(node_link_filter)
    if node_link is None:
        msg = f"No node link found for filter {node_link_filter!r}"
        raise ValueError(msg)
    return node_link


async def read_node_link_or_none(node_link_filter: NodeLinkFilter[Any] | Filter) -> NodeLink | None:
    """Read a parent-to-child link between nodes."""
    async with current_session() as session:
        result = await session.execute(select(NodeLink).where(node_link_filter.create()))
        return result.scalar_one_or_none()


async def read_node_links(node_link_filter: NodeLinkFilter[Any] | Filter) -> Sequence[NodeLink]:
    """Read parent-to-child links between nodes."""
    cmd = select(NodeLink).where(node_link_filter.create())
    async with current_session() as session:
        return (await session.execute(cmd)).scalars().all()


async def write_node_link(node_link: NodeLink) -> None:
    """Create a parent-to-child link between nodes.

    Updates the existing child node's node_parent_id.
    """
    await write_node_links([node_link])


async def write_node_links(node_links: Sequence[NodeLink]) -> None:
    """Create parent-to-child links between nodes.

    Updates the existing child node's node_parent_id.
    """
    async with current_session() as session:
        session.add_all(node_links)
        await session.commit()


async def delete_node_links(node_filter: NodeFilter[Any] | Filter) -> None:
    """Delete parent-to-child links between nodes."""
    async with current_session() as session:
        delete_cmd = delete(NodeLink).where(node_filter.create())
        await session.execute(delete_cmd)
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
