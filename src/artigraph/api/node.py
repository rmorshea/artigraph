from __future__ import annotations

from collections.abc import Collection
from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import fields
from functools import wraps
from typing import (
    Any,
    AsyncIterator,
    Callable,
    Iterable,
    Literal,
    Protocol,
    Sequence,
    TypeVar,
    overload,
)

from sqlalchemy import Row, case, delete, join, select, update
from sqlalchemy.orm import aliased
from typing_extensions import ParamSpec, TypeGuard

from artigraph.db import current_session, session_context
from artigraph.orm.node import NODE_TYPE_BY_POLYMORPHIC_IDENTITY, Node

P = ParamSpec("P")
R_co = TypeVar("R_co", covariant=True)
N = TypeVar("N", bound=Node)

_CURRENT_NODE_ID: ContextVar[int | None] = ContextVar("CURRENT_NODE_ID", default=None)


@overload
def current_node_id(*, allow_none: Literal[True]) -> int | None:
    ...


@overload
def current_node_id(*, allow_none: Literal[False] = ...) -> int:
    ...


def current_node_id(*, allow_none: bool = False) -> int | None:
    """Get the current span ID."""
    span_id = _CURRENT_NODE_ID.get()
    if span_id is None and not allow_none:
        msg = "No span is currently active."
        raise RuntimeError(msg)
    return span_id


def with_current_node_id(func: _NodeFunc[P, R_co]) -> _CurrentNodeFunc[P, R_co]:
    @wraps(func)
    async def wrapper(node_id: int | Literal["current"], *args: P.args, **kwargs: P.kwargs) -> R_co:
        return await func(
            current_node_id() if node_id == "current" else node_id,
            *args,
            **kwargs,
        )

    return wrapper


@asynccontextmanager
async def create_current(
    node_type: Callable[P, N],
    *args: P.args,
    **kwargs: P.kwargs,
) -> AsyncIterator[N]:
    """Create a new node and set it as the current node."""
    kwargs.setdefault("node_parent_id", current_node_id(allow_none=True))
    node = node_type(*args, **kwargs)
    async with session_context(expire_on_commit=False) as session:
        session.add(node)
        await session.commit()
        await session.refresh(node)
    last_span_token = _CURRENT_NODE_ID.set(node.node_id)
    try:
        yield node
    finally:
        _CURRENT_NODE_ID.reset(last_span_token)


def group_nodes_by_parent_id(nodes: Sequence[N]) -> dict[int | None, list[N]]:
    """Group nodes by their parent ID."""
    grouped_nodes: dict[int | None, list[N]] = {}
    for node in nodes:
        grouped_nodes.setdefault(node.node_parent_id, []).append(node)
    return grouped_nodes


def is_node_type(node: Node, node_type: type[N]) -> TypeGuard[N]:
    """Check if a node is of a given type."""
    return node.node_type == node_type.polymorphic_identity


async def read_node_exists(node_id: int) -> bool:
    """Check if a node exists."""
    return bool(await read_node(node_id, allow_none=True))


async def read_nodes_exist(node_ids: Sequence[int]) -> bool:
    """Check if nodes exist."""
    return all(await read_nodes(node_ids, allow_none=True))


@overload
async def read_node(node_id: int, *, allow_none: bool) -> Node | None:
    ...


@overload
async def read_node(node_id: int, *, allow_none: Literal[False] = ...) -> Node:
    ...


async def read_node(node_id: int, *, allow_none: bool = False) -> Node | None:
    """Read a node by its ID."""
    return (await read_nodes([node_id], allow_none=allow_none))[0]


@overload
async def read_nodes(node_ids: Sequence[int], *, allow_none: bool) -> Sequence[Node | None]:
    ...


@overload
async def read_nodes(
    node_ids: Sequence[int], *, allow_none: Literal[False] = ...
) -> Sequence[Node]:
    ...


async def read_nodes(node_ids: Sequence[int], *, allow_none: bool = False) -> Sequence[Node | None]:
    """Read nodes by their IDs."""
    cmd = select(Node.__table__).where(Node.node_id.in_(node_ids))
    async with current_session() as session:
        result = await session.execute(cmd)
        nodes_by_id = {n.node_id: n for n in load_nodes_from_rows(result.all())}

    if not allow_none and len(nodes_by_id) != len(node_ids):
        missing = set(node_ids) - set(nodes_by_id)
        msg = f"Could not find node IDs: {list(missing)}"
        raise ValueError(msg)

    return [nodes_by_id.get(node_id) for node_id in node_ids]  # type: ignore


async def delete_node(node_id: int, *, descendants: bool = True) -> None:
    """Delete a node and optionally their descendants."""
    nodes_ids = [node_id]
    if descendants:  # nocov (FIXME: actually covered but not detected)
        nodes_ids.extend(n.node_id for n in await read_descendant_nodes(node_id))
    cmd = delete(Node).where(Node.node_id.in_(nodes_ids))
    async with current_session() as session:
        await session.execute(cmd)
        await session.commit()


async def write_node(node: Node, *, refresh_attributes: Sequence[str] = ()) -> Node:
    """Create a node."""
    return (await write_nodes([node], refresh_attributes=refresh_attributes))[0]


async def write_nodes(
    nodes: Collection[Node], *, refresh_attributes: Sequence[str]
) -> Sequence[Node]:
    """Create nodes and, if given, refresh their attributes."""
    async with current_session() as session:  # nocov (FIXME: actually covered but not detected)
        session.add_all(nodes)
        await session.commit()
        if refresh_attributes:
            # We can't do this in asyncio.gather() because of issues with concurrent connections:
            # https://docs.sqlalchemy.org/en/20/errors.html#illegalstatechangeerror-and-concurrency-exceptions
            for n in nodes:
                await session.refresh(n, refresh_attributes)
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


async def read_child_nodes(node_id: int, *node_types: type[N]) -> Sequence[N]:
    """Read the direct children of a node."""
    cmd = select(Node.__table__).where(Node.node_parent_id == node_id)
    if node_types:
        cmd = cmd.where(Node.node_type.in_([n.polymorphic_identity for n in node_types]))
    async with current_session() as session:
        result = await session.execute(cmd)
        return load_nodes_from_rows(result.all())


async def read_parent_node(node_id: int, node_type: type[N] = Node) -> N | None:
    """Read the direct parent of a node."""
    async with current_session() as session:
        node_cmd = select(node_type).where(node_type.node_id == node_id)
        result = await session.execute(node_cmd)
        node = result.scalar_one()
        parent_cmd = select(node_type).where(node_type.node_id == node.node_parent_id)
        result = await session.execute(parent_cmd)
        return result.scalar_one_or_none()


async def read_descendant_nodes(node_id: int, *node_types: type[N]) -> Sequence[N]:
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


async def read_ancestor_nodes(node_id: int, *node_types: type[N]) -> Sequence[N]:
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

    if node_types:  # nocov (FIXME: actually covered but not detected)
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
        node_type = NODE_TYPE_BY_POLYMORPHIC_IDENTITY[r.node_type]
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


class _NodeFunc(Protocol[P, R_co]):
    async def __call__(
        self,
        node_id: int,
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> R_co:
        ...


class _CurrentNodeFunc(Protocol[P, R_co]):
    async def __call__(
        self,
        node_id: int | Literal["current"],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> R_co:
        ...
