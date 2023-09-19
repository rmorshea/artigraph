from __future__ import annotations

from asyncio import iscoroutinefunction
from contextlib import asynccontextmanager
from contextvars import ContextVar
from functools import partial, wraps
from inspect import isfunction, signature
from typing import (
    Any,
    AsyncContextManager,
    AsyncIterator,
    Callable,
    Collection,
    Concatenate,
    ParamSpec,
    Protocol,
    TypeVar,
    cast,
)

from artigraph.core.api.artifact import Artifact
from artigraph.core.api.base import GraphBase
from artigraph.core.api.funcs import write_many
from artigraph.core.api.link import NodeLink
from artigraph.core.api.node import Node
from artigraph.core.model.base import GraphModel
from artigraph.core.utils.anysync import AnySyncContextManager
from artigraph.core.utils.type_hints import TypeHintMetadata, get_artigraph_type_hint_metadata

P = ParamSpec("P")
R = TypeVar("R")
N = TypeVar("N", bound=Node)
NodeContext = Callable[[Node, str | None], AsyncContextManager[Node] | None]

_CURRENT_NODE: ContextVar[Node | None] = ContextVar("CURRENT_NODE", default=None)
_CURRENT_LABELS: ContextVar[frozenset[str]] = ContextVar("CURRENT_LABELS", default=frozenset())


def start_trace(node: Node, label: str | None = None) -> TraceNode:
    """Begin tracing a call graph"""
    return TraceNode(node, label)


@asynccontextmanager
async def default_node_context(parent: Node, label: str | None) -> AsyncIterator[Node]:
    child = Node()
    await write_many.a(
        [
            child,
            NodeLink(
                parent_id=parent.node_id,
                child_id=child.node_id,
                label=label,
            ),
        ]
    )
    yield child


def trace_function(
    *,
    node_type: Callable[[], Node] = Node,
    is_method: bool = False,
    do_not_save: Collection[str] = (),
) -> Callable[[Callable[P, R]], GraphTracer[P, R]]:
    """Capture the inputs and outputs of a function using Artigraph"""

    do_not_save = set(do_not_save)

    def decorator(func: Callable[P, R]) -> GraphTracer[P, R]:
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
                async with start_trace(node_type(), label) as node:
                    to_write: list[GraphBase] = []
                    try:
                        to_write.extend(
                            _create_linked_values(
                                node,
                                inputs,
                                hint_info,
                                do_not_save,
                            )
                        )
                        output = await func(*args, **kwargs)
                        to_write.extend(
                            _create_linked_values(
                                node,
                                {"return": output},
                                hint_info,
                                do_not_save,
                            )
                        )
                        return output
                    finally:
                        await write_many.a(to_write)

            wrapper = _awrapper

        elif isfunction(func):

            @wraps(func)
            def _swrapper(label: str | None, *args: P.args, **kwargs: P.kwargs) -> Any:
                label, inputs = _create_label_and_inputs(label, args, kwargs)
                with start_trace(node_type(), label) as node:
                    to_write: list[GraphBase] = []
                    try:
                        to_write.extend(
                            _create_linked_values(
                                node,
                                inputs,
                                hint_info,
                                do_not_save,
                            )
                        )
                        output = func(*args, **kwargs)
                        to_write.extend(
                            _create_linked_values(
                                node,
                                {"return": output},
                                hint_info,
                                do_not_save,
                            )
                        )
                        return output
                    finally:
                        write_many.s(to_write)

            wrapper = _swrapper

        else:
            msg = f"Expected a function, got {type(func)}"
            raise TypeError(msg)

        tracer = partial(wrapper, None)
        tracer.labeled = wrapper  # type: ignore

        return cast(GraphTracer[P, R], tracer)

    return decorator


def current_node() -> Node | None:
    """Get the current node"""
    return _CURRENT_NODE.get()


class GraphTracer(Protocol[P, R]):
    """A function that traces its inputs and outputs"""

    __call__: Callable[P, R]
    labeled: Callable[Concatenate[str | None, P], R]


class TraceNode(AnySyncContextManager[N]):
    def __init__(self, node: N, label: str | None = None) -> None:
        self.node = node
        self.label = label
        self.last_node = _CURRENT_NODE.get()
        self.last_labels = _CURRENT_LABELS.get()

    async def _aenter(self) -> N:
        to_write: list[GraphBase] = [self.node]
        if self.last_node is not None:
            to_write.append(
                NodeLink(
                    parent_id=self.last_node.node_id,
                    child_id=self.node.node_id,
                    label=self.label,
                )
            )
        await write_many.a(to_write)
        return self.node

    def _enter(self) -> None:
        self._node_token = _CURRENT_NODE.set(self.node)
        if self.label is not None:
            self._label_token = _CURRENT_LABELS.set(self.last_labels.union({self.label}))
        else:
            self._label_token = None

    def _exit(self) -> None:
        _CURRENT_NODE.reset(self._node_token)
        if self._label_token is not None:
            _CURRENT_LABELS.reset(self._label_token)


def _create_linked_values(
    parent: Node,
    values: dict[str, Any],
    hint_info: dict[str, TypeHintMetadata],
    do_not_save: set[str],
) -> list[GraphBase]:
    """Create a node link for each value in the given dict"""
    records: list[GraphBase] = []
    for k, v in values.items():
        if k in do_not_save:
            continue
        if isinstance(v, GraphBase):
            graph_obj = v
            if isinstance(v, Node):
                child_id = v.node_id
            elif isinstance(v, GraphModel):
                child_id = v.graph_node_id
            else:
                msg = f"Unexpected graph object: {type(v)}"
                raise TypeError(msg)
        else:
            info = hint_info.get(k, TypeHintMetadata())
            for serializer in info.serializers:
                if isinstance(v, serializer.types):
                    break
            else:
                serializer = None
            graph_obj = Artifact(value=v, serializer=serializer, storage=info.storage)
            child_id = graph_obj.node_id
        artlink = NodeLink(
            parent_id=parent.node_id,
            child_id=child_id,
            label=k,
        )
        records.extend([graph_obj, artlink])
    return records
