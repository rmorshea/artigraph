from __future__ import annotations

from asyncio import iscoroutinefunction
from contextvars import ContextVar
from functools import wraps
from inspect import isfunction, signature
from typing import (
    Any,
    AsyncContextManager,
    AsyncIterator,
    Callable,
    Concatenate,
    Iterable,
    Iterator,
    ParamSpec,
    Protocol,
    TypeVar,
)

from artigraph.core.api.artifact import Artifact
from artigraph.core.api.base import GraphBase
from artigraph.core.api.funcs import write_many, write_one
from artigraph.core.api.link import NodeLink
from artigraph.core.api.node import Node
from artigraph.core.model.base import GraphModel
from artigraph.core.utils.anysync import anysynccontextmanager
from artigraph.core.utils.type_hints import AnnotationInfo, get_annotation_info

P = ParamSpec("P")
R = TypeVar("R")
NodeContext = Callable[[Node, str | None], AsyncContextManager[Node] | None]

_CURRENT_NODE: ContextVar[Node] = ContextVar("CURRENT_NODE")
_CURRENT_LABELS: ContextVar[frozenset[str]] = ContextVar("CURRENT_LABELS", default=frozenset())


@anysynccontextmanager
async def trace_graph(node: Node) -> AsyncIterator[Node]:
    """Begin tracing a graph"""
    node_token = _CURRENT_NODE.set(node)
    labels_token = _CURRENT_LABELS.set(frozenset())
    try:
        await write_one.a(node)
        yield node
    finally:
        _CURRENT_NODE.reset(node_token)
        _CURRENT_LABELS.reset(labels_token)


@anysynccontextmanager
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


def graph_tracer(
    *,
    node_context: NodeContext = default_node_context,
    is_method: bool = False,
) -> Callable[[Callable[P, R]], GraphTracer[P, R]]:
    """Capture the inputs and outputs of a function using Artigraph"""

    def decorator(func: Callable[P, R]) -> GraphTracer[P, R]:
        sig = signature(func)
        hint_info = get_annotation_info(func)

        if iscoroutinefunction(func):

            @wraps(func)
            async def wrapper(label: str, inputs: dict[str, Any], *args: Any, **kwargs: Any) -> Any:
                async with start_trace_node(node_context, label) as add:
                    add(_create_linked_values(current_node(), inputs, hint_info))
                    output = await func(*args, **kwargs)
                    add(_create_linked_values(current_node(), {"return": output}, hint_info))
                    return output

        elif isfunction(func):

            @wraps(func)
            def wrapper(label: str, inputs: dict[str, Any], *args: Any, **kwargs: Any) -> Any:
                with start_trace_node(node_context, label) as add:
                    add(_create_linked_values(current_node(), inputs))
                    output = func(*args, **kwargs)
                    add(_create_linked_values(current_node(), {"return": output}))
                    return output

        else:
            msg = f"Expected a function, got {type(func)}"
            raise TypeError(msg)

        def common_wrapper(label: str | None, *args: Any, **kwargs: Any) -> Any:
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
            return wrapper(full_label, inputs, *args, **kwargs)

        @wraps(func)
        def tracer(*args: Any, **kwargs: Any) -> Any:
            return common_wrapper(None, *args, **kwargs)

        tracer.labeled = common_wrapper  # type: ignore

        return tracer

    return decorator


class GraphTracer(Protocol[P, R]):
    """A function that traces its inputs and outputs"""

    __call__: Callable[P, R]
    labeled: Callable[Concatenate[str | None, P], R]


def current_node() -> Node:
    """Get the current node"""
    try:
        return _CURRENT_NODE.get()
    except LookupError:
        msg = "No current node - did you forget to use begin_graph?"
        raise RuntimeError(msg) from None


@anysynccontextmanager
async def start_trace_node(
    node_context: NodeContext = default_node_context,
    label: str | None = None,
) -> Iterator[Callable[[Iterable[GraphBase]], None]]:
    """Start a new node"""
    last_node = current_node()

    last_labels = _CURRENT_LABELS.get()
    if label in last_labels:
        msg = f"Duplicate label: {label}"
        raise ValueError(msg)

    to_write: list[GraphBase] = []
    try:
        async with node_context(last_node, label) as this_node:
            _CURRENT_NODE.set(this_node)
            _CURRENT_LABELS.set(last_labels | {label})
            yield to_write.extend
    finally:
        _CURRENT_LABELS.set(last_labels)
        _CURRENT_NODE.set(last_node)
        await write_many.a(to_write)


def _create_linked_values(
    parent: Node,
    values: dict[str, Any],
    hint_info: dict[str, AnnotationInfo],
) -> list[GraphBase]:
    """Create a node link for each value in the given dict"""
    records: list[GraphBase] = []
    for k, v in values.items():
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
            info = hint_info.get(k, AnnotationInfo())
            for serializer in info.serializers:
                if serializer.serializable(v):
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
