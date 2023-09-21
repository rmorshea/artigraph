from __future__ import annotations

from asyncio import iscoroutinefunction
from contextvars import ContextVar
from functools import wraps
from inspect import isfunction, signature
from typing import (
    Any,
    AsyncContextManager,
    Callable,
    Collection,
    TypeVar,
    cast,
)

from typing_extensions import Self

from artigraph.core.api.artifact import Artifact, SaveSpec
from artigraph.core.api.base import GraphObject
from artigraph.core.api.funcs import write_many
from artigraph.core.api.link import Link
from artigraph.core.api.node import Node
from artigraph.core.serializer.base import Serializer
from artigraph.core.storage.base import Storage
from artigraph.core.utils.anysync import AnySyncContextManager
from artigraph.core.utils.type_hints import get_save_specs_from_type_hints

N = TypeVar("N", bound=Node)
F = TypeVar("F", bound=Callable[..., Any])
NodeContext = Callable[[Node, str | None], AsyncContextManager[Node] | None]

_CURRENT_LINKER: ContextVar[Linker | None] = ContextVar("CURRENT_LINKER", default=None)


def current_linker() -> Linker:
    """Get the current linker"""
    linker = _CURRENT_LINKER.get()
    if linker is None:  # nocov
        msg = "No linker is currently active"
        raise RuntimeError(msg)
    return linker


def linked(
    *,
    node_type: Callable[[], Node] = Node,
    is_method: bool = False,
    include: str | Collection[str] = (),
    exclude: str | Collection[str] = (),
) -> Callable[[F], F]:
    """Capture the inputs and outputs of a function using Artigraph"""

    if include and exclude:  # nocov
        msg = "Cannot specify both only_save and do_not_save"
        raise ValueError(msg)

    include = {include} if isinstance(include, str) else set(include)
    exclude = {exclude} if isinstance(exclude, str) else set(exclude)
    call_id = 0

    def decorator(func: F) -> F:
        sig = signature(func)
        hint_info = get_save_specs_from_type_hints(func)

        def _create_label_and_inputs(args, kwargs):
            nonlocal call_id
            call_id += 1
            full_label = f"{func.__qualname__}[{call_id}]"
            bound_args = sig.bind_partial(*args, **kwargs)
            inputs = {
                k: v
                for i, (k, v) in enumerate(bound_args.arguments.items())
                if not is_method or i > 1
            }
            return full_label, inputs

        if iscoroutinefunction(func):

            @wraps(func)
            async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                # Check that there's an active linker. Without one, it's possible to
                # produce orphaned nodes. It seems better to prevent that.
                current_linker()

                label, inputs = _create_label_and_inputs(args, kwargs)
                async with Linker(node_type(), label) as linker:
                    output = await func(*args, **kwargs)
                    values = {"return": output, **inputs}
                    for k, v in _create_graph_objects(
                        values,
                        hint_info,
                        include,
                        exclude,
                    ).items():
                        linker.link(v, k)
                    return output

            return cast(F, async_wrapper)

        elif isfunction(func):

            @wraps(func)
            def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
                nonlocal call_id

                # Check that there's an active linker. Without one, it's possible to
                # produce orphaned nodes. It seems better to prevent that.
                current_linker()

                call_id += 1
                label, inputs = _create_label_and_inputs(args, kwargs)
                with Linker(node_type(), label) as linker:
                    output = func(*args, **kwargs)
                    values = {"return": output, **inputs}
                    for k, v in _create_graph_objects(
                        values,
                        hint_info,
                        include,
                        exclude,
                    ).items():
                        linker.link(v, k)
                    return output

            return cast(F, sync_wrapper)

        else:  # nocov
            msg = f"Expected a function, got {type(func)}"
            raise TypeError(msg)

    return decorator


class Linker(AnySyncContextManager["Linker"]):
    """A context manager for linking graph objects together"""

    def __init__(self, node: GraphObject, label: str | None = None) -> None:
        self.node = node
        self.label = label
        self._labels: set[str] = set()
        self._write_on_enter: list[GraphObject] = [self.node]
        self._write_on_exit: list[GraphObject] = []

    def link(
        self,
        value: Any,
        label: str | None = None,
        storage: Storage | None = None,
        serializer: Serializer | None = None,
    ) -> None:
        """Link a graph object to the current node"""
        if label is not None:
            if label in self._labels:
                msg = f"Label {label} already exists for {self.node}"
                raise ValueError(msg)
            self._labels.add(label)

        if isinstance(value, GraphObject):
            graph_obj = value
            if storage is not None or serializer is not None:
                msg = "Cannot specify storage or serializer when linking a GraphObject"
                raise ValueError(msg)
        else:
            graph_obj = Artifact(value=value, storage=storage, serializer=serializer)

        self._write_on_exit.extend(
            [
                graph_obj,
                Link(
                    source_id=self.node.graph_id,
                    target_id=graph_obj.graph_id,
                    label=label,
                ),
            ]
        )

    async def _aenter(self) -> Self:
        await write_many.a(self._write_on_enter)
        return self

    async def _aexit(self, *_: Any) -> None:
        await write_many.a(self._write_on_exit)

    def _enter(self) -> None:
        self.parent = _CURRENT_LINKER.get()
        if self.parent is not None:
            self._write_on_enter.append(
                Link(
                    source_id=self.parent.node.graph_id,
                    target_id=self.node.graph_id,
                    label=self.label,
                )
            )
        self._reset_parent = _CURRENT_LINKER.set(self)

    def _exit(self) -> None:
        _CURRENT_LINKER.reset(self._reset_parent)


def _create_graph_objects(
    values: dict[str, Any],
    save_specs: dict[str, SaveSpec],
    include: set[str],
    exclude: set[str],
) -> dict[str, GraphObject]:
    """Create a node link for each value in the given dict"""
    records: dict[str, GraphObject] = {}
    for k, v in values.items():
        if k in exclude or (include and k not in include):
            continue
        if isinstance(v, GraphObject):
            graph_obj = v
        elif k in save_specs:
            spec = save_specs[k]
            graph_obj = spec.create_artifact(v, strict=not spec.is_empty())
        else:
            graph_obj = Artifact(value=v)
        records[k] = graph_obj
    return records
