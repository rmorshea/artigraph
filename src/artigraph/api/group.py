from __future__ import annotations

from contextlib import asynccontextmanager
from contextvars import ContextVar
from functools import wraps
from typing import AsyncIterator, Literal, Protocol, TypeVar, cast, overload

from typing_extensions import ParamSpec

from artigraph.db import session_context
from artigraph.orm.group import Group

P = ParamSpec("P")
R_co = TypeVar("R_co", covariant=True)
G = TypeVar("G", bound=Group)

_CURRENT_GROUP_ID: ContextVar[int | None] = ContextVar("CURRENT_GROUP_ID", default=None)


@overload
def current_group_id(*, allow_none: Literal[True]) -> int | None:
    ...


@overload
def current_group_id(*, allow_none: Literal[False] = ...) -> int:
    ...


def current_group_id(*, allow_none: bool = False) -> int | None:
    """Get the current span ID."""
    span_id = _CURRENT_GROUP_ID.get()
    if span_id is None and not allow_none:
        msg = "No span is currently active."
        raise RuntimeError(msg)
    return span_id


def with_group_id(func: _GroupFunc[P, R_co]) -> _CurrentGroupFunc[P, R_co]:
    @wraps(func)
    async def wrapper(span_id: int | Literal["current"], *args: P.args, **kwargs: P.kwargs) -> R_co:
        return await func(
            current_group_id() if span_id == "current" else span_id,
            *args,
            **kwargs,
        )

    return wrapper


@asynccontextmanager
async def group(group: G | None = None, label: str | None = None) -> AsyncIterator[G]:
    """Create a context manager for a group."""
    if group is not None and label is not None:
        msg = "Cannot specify both span and label."
        raise ValueError(msg)

    child_span = cast(G, Group(node_parent_id=None, span_label=label) if group is None else group)

    parent_span_id = _CURRENT_GROUP_ID.get()
    existing_span = child_span.node_id is not None
    if not existing_span:  # nocov (FIXME: actually covered but not detected)
        async with session_context(expire_on_commit=False) as session:
            child_span.node_parent_id = parent_span_id
            session.add(child_span)
            await session.commit()
            await session.refresh(child_span)
    last_span_token = _CURRENT_GROUP_ID.set(child_span.node_id)
    try:
        yield child_span
    finally:
        _CURRENT_GROUP_ID.reset(last_span_token)
        if not existing_span:  # nocov (FIXME: actually covered but not detected)
            async with session_context(expire_on_commit=False) as session:
                session.add(child_span)
                await session.commit()


class _GroupFunc(Protocol[P, R_co]):
    async def __call__(self, span_id: int, *args: P.args, **kwargs: P.kwargs) -> R_co:
        ...


class _CurrentGroupFunc(Protocol[P, R_co]):
    async def __call__(
        self, span_id: int | Literal["current"], *args: P.args, **kwargs: P.kwargs
    ) -> R_co:
        ...
