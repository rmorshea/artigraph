from __future__ import annotations

from asyncio import iscoroutinefunction
from contextvars import ContextVar
from functools import partial, wraps
from inspect import isfunction, signature
from typing import (
    Any,
    AsyncContextManager,
    Callable,
    Collection,
    Concatenate,
    ParamSpec,
    Protocol,
    TypeVar,
    cast,
)

from typing_extensions import Self

from artigraph.core.api.artifact import Artifact
from artigraph.core.api.base import GraphObject
from artigraph.core.api.funcs import write_many
from artigraph.core.api.link import Link
from artigraph.core.api.node import Node
from artigraph.core.serializer.base import Serializer
from artigraph.core.storage.base import Storage
from artigraph.core.utils.anysync import AnySyncContextManager
from artigraph.core.utils.type_hints import TypeHintMetadata, get_artigraph_type_hint_metadata

P = ParamSpec("P")
R = TypeVar("R")
N = TypeVar("N", bound=Node)
NodeContext = Callable[[Node, str | None], AsyncContextManager[Node] | None]

_CURRENT_LINKER: ContextVar[Linker | None] = ContextVar("CURRENT_LINKER", default=None)


def get_linker() -> Linker:
    """Get the current linker"""
    linker = _CURRENT_LINKER.get()
    if linker is None:
        msg = "No linker is currently active"
        raise RuntimeError(msg)
    return linker


def linked(
    *,
    node_type: Callable[[], Node] = Node,
    is_method: bool = False,
    do_not_save: Collection[str] = (),
) -> Callable[[Callable[P, R]], LinkedFunc[P, R]]:
    """Capture the inputs and outputs of a function using Artigraph"""

    do_not_save = set(do_not_save)

    def decorator(func: Callable[P, R]) -> LinkedFunc[P, R]:
        sig = signature(func)
        hint_info = get_artigraph_type_hint_metadata(func)

        def _create_label_and_inputs(label, args, kwargs):
            if label is not None:
                full_label = f"{func.__qualname__}[{label}]"
            else:
                full_label = func.__qualname__
            bound_args = sig.bind_partial(*args, **kwargs)
            inputs = {
                k: v
                for i, (k, v) in enumerate(bound_args.arguments.items())
                if not is_method or i > 1
            }
            return full_label, inputs

        if iscoroutinefunction(func):

            @wraps(func)
            async def _awrapper(label: str | None, *args: P.args, **kwargs: P.kwargs) -> Any:
                label, inputs = _create_label_and_inputs(label, args, kwargs)
                async with Linker(node_type(), label) as linker:
                    output = await func(*args, **kwargs)
                    values = {"return": output, **inputs}
                    for k, v in _create_graph_objects(values, hint_info, do_not_save).items():
                        linker.link(v, k)
                    return output

            wrapper = _awrapper

        elif isfunction(func):

            @wraps(func)
            def _swrapper(label: str | None, *args: P.args, **kwargs: P.kwargs) -> Any:
                label, inputs = _create_label_and_inputs(label, args, kwargs)
                with Linker(node_type(), label) as linker:
                    output = func(*args, **kwargs)
                    values = {"return": output, **inputs}
                    for k, v in _create_graph_objects(values, hint_info, do_not_save).items():
                        linker.link(v, k)
                    return output

            wrapper = _swrapper

        else:  # nocov
            msg = f"Expected a function, got {type(func)}"
            raise TypeError(msg)

        tracer = partial(wrapper, None)
        tracer.label = wrapper  # type: ignore

        return cast(LinkedFunc[P, R], tracer)

    return decorator


class Linker(AnySyncContextManager["Linker"]):
    """A context manager for linking graph objects together"""

    def __init__(self, node: N, label: str | None = None) -> None:
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
        if label is not None and label in self._labels:
            msg = f"Label {label} already exists for {self.node}"
            raise ValueError(msg)

        if isinstance(value, GraphObject):
            graph_obj = value
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


class LinkedFunc(Protocol[P, R]):
    """A function that traces its inputs and outputs"""

    __call__: Callable[P, R]
    label: Callable[Concatenate[str | None, P], R]


def _create_graph_objects(
    values: dict[str, Any],
    hint_info: dict[str, TypeHintMetadata],
    do_not_save: set[str],
) -> dict[str, GraphObject]:
    """Create a node link for each value in the given dict"""
    records: dict[str, GraphObject] = {}
    for k, v in values.items():
        if k in do_not_save:
            continue
        if isinstance(v, GraphObject):
            graph_obj = v
        else:
            if k in hint_info:
                hinted_serializers = hint_info[k].serializers
                if hinted_serializers:
                    for serializer in hint_info[k].serializers:
                        if isinstance(v, serializer.types):
                            break
                    else:
                        allowed_types = [s.types for s in hint_info[k].serializers]
                        msg = f"Expected {k} to be one of {allowed_types}, got {type(v)}"
                        raise TypeError(msg)
                else:
                    serializer = None
                storage = hint_info[k].storage
            else:
                serializer = None
                storage = None
            graph_obj = Artifact(value=v, serializer=serializer, storage=storage)
        records[k] = graph_obj
    return records
