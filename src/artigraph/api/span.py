from __future__ import annotations

from contextlib import asynccontextmanager
from contextvars import ContextVar
from datetime import datetime, timezone
from functools import wraps
from typing import AsyncIterator, Literal, Protocol, Sequence, TypeVar, cast, overload

from sqlalchemy import select
from typing_extensions import ParamSpec

from artigraph.api.artifact_model import ArtifactModel
from artigraph.api.node import (
    read_ancestor_nodes,
    read_child_nodes,
    read_descendant_nodes,
    read_parent_node,
)
from artigraph.db import current_session, session_context
from artigraph.orm.artifact import BaseArtifact
from artigraph.orm.span import Span
from artigraph.utils import SessionBatch

P = ParamSpec("P")
R_co = TypeVar("R_co", covariant=True)
S = TypeVar("S", bound=Span)

_CURRENT_SPAN_ID: ContextVar[int | None] = ContextVar("CURRENT_SPAN_ID", default=None)


@overload
def get_current_span_id(*, allow_none: Literal[True]) -> int | None:
    ...


@overload
def get_current_span_id(*, allow_none: Literal[False] = ...) -> int:
    ...


def get_current_span_id(*, allow_none: bool = False) -> int | None:
    """Get the current span ID."""
    span_id = _CURRENT_SPAN_ID.get()
    if span_id is None and not allow_none:
        msg = "No span is currently active."
        raise RuntimeError(msg)
    return span_id


@asynccontextmanager
async def span_context(span: S | None = None, label: str | None = None) -> AsyncIterator[S]:
    """Create a context manager for a span.

    If the span does not exist in the database, it will be created - its "opened at" and
    "closed at" times set at the beginning and end of the context respectively.
    """
    if span is not None and label is not None:
        msg = "Cannot specify both span and label."
        raise ValueError(msg)

    child_span = cast(S, Span(node_parent_id=None, span_label=label) if span is None else span)

    parent_span_id = _CURRENT_SPAN_ID.get()
    existing_span = child_span.node_id is not None
    if not existing_span:  # nocov (FIXME: actually covered but not detected)
        async with session_context(expire_on_commit=False) as session:
            child_span.node_parent_id = parent_span_id
            child_span.span_opened_at = datetime.now(timezone.utc)
            session.add(child_span)
            await session.commit()
            await session.refresh(child_span)
    last_span_token = _CURRENT_SPAN_ID.set(child_span.node_id)
    try:
        yield child_span
    finally:
        _CURRENT_SPAN_ID.reset(last_span_token)
        if not existing_span:  # nocov (FIXME: actually covered but not detected)
            async with session_context(expire_on_commit=False) as session:
                child_span.span_closed_at = datetime.now(timezone.utc)
                session.add(child_span)
                await session.commit()


def with_current_span_id(func: _SpanFunc[P, R_co]) -> _CurrentSpanFunc[P, R_co]:
    @wraps(func)
    async def wrapper(span_id: int | Literal["current"], *args: P.args, **kwargs: P.kwargs) -> R_co:
        return await func(
            get_current_span_id() if span_id == "current" else span_id,
            *args,
            **kwargs,
        )

    return wrapper


@with_current_span_id
async def read_child_spans(span_id: int) -> Sequence[Span]:
    """Get direct children of a span."""
    return await read_child_nodes(span_id, Span)


@with_current_span_id
async def read_descendant_spans(span_id: int) -> Sequence[Span]:
    """Get all descendants of a span."""
    return await read_descendant_nodes(span_id, Span)


@with_current_span_id
async def read_ancestor_spans(span_id: int) -> Sequence[Span]:
    """Get all ancestors of a span."""
    return await read_ancestor_nodes(span_id, Span)


@with_current_span_id
async def read_parent_span(span_id: int) -> Span | None:
    """Get the parent of a span."""
    return await read_parent_node(span_id, Span)


@with_current_span_id
async def create_span_artifact(span_id: int, *, label: str, artifact: ArtifactModel) -> int:
    """Add an artifact to the span and return its ID"""
    return await artifact.create(label, parent_id=span_id)


@with_current_span_id
async def create_span_artifacts(
    span_id: int, artifacts: dict[str, ArtifactModel]
) -> dict[str, int]:
    """Add artifacts to the span and return their IDs."""
    return {
        # FIXME: Not really sure why we can't do this concurrently.
        # If we do, we sometimes don't write all records.
        k: await create_span_artifact(span_id, label=k, artifact=a)
        for k, a in artifacts.items()
    }


@with_current_span_id
async def read_span_artifact(span_id: int, *, label: str) -> ArtifactModel:
    """Load an artifact for this span."""
    async with current_session() as session:
        cmd = (
            select(BaseArtifact.node_id)
            .where(BaseArtifact.artifact_label == label)
            .where(BaseArtifact.node_parent_id == span_id)
        )
        result = await session.execute(cmd)
        node_id = result.scalar_one()
        return await ArtifactModel.read(node_id)


@with_current_span_id
async def read_span_artifacts(span_id: int) -> dict[str, ArtifactModel]:
    """Load all artifacts for this span."""
    artifact_models: dict[str, ArtifactModel] = {}
    async with current_session() as session:
        cmd = (
            select(BaseArtifact.node_id, BaseArtifact.artifact_label)
            .where(BaseArtifact.artifact_label.is_not(None))
            .where(BaseArtifact.node_parent_id == span_id)
        )
        result = await session.execute(cmd)
        node_ids_and_labels = list(result.all())

    if not node_ids_and_labels:
        return artifact_models

    node_ids, artifact_labels = zip(*node_ids_and_labels)
    for label, model in zip(
        artifact_labels,
        await SessionBatch().map(ArtifactModel.read, node_ids).gather(),
    ):
        artifact_models[label] = model

    return artifact_models


class _SpanFunc(Protocol[P, R_co]):
    async def __call__(self, span_id: int, *args: P.args, **kwargs: P.kwargs) -> R_co:
        ...


class _CurrentSpanFunc(Protocol[P, R_co]):
    async def __call__(
        self, span_id: int | Literal["current"], *args: P.args, **kwargs: P.kwargs
    ) -> R_co:
        ...
