from dataclasses import replace
from typing import Annotated, Any

import numpy as np
import pandas as pd

from artigraph.core.api.filter import NodeFilter, NodeLinkFilter
from artigraph.core.api.funcs import exists, read, read_one
from artigraph.core.api.link import NodeLink
from artigraph.core.api.node import Node
from artigraph.core.graph.trace import current_node, trace_function, trace_node
from artigraph.core.serializer.json import json_sorted_serializer
from artigraph.extras.numpy import array_serializer
from artigraph.extras.pandas import dataframe_serializer
from tests.common.model import SimpleDataclassModel


@trace_function()
async def simple_function(x: int, y: int) -> int:
    return x + y


@trace_function()
async def function_with_graph_objs(obj: SimpleDataclassModel) -> SimpleDataclassModel:
    return replace(obj, x=obj.x + 1)


@trace_function()
async def function_with_annotated_serializer(data: Annotated[Any, json_sorted_serializer]) -> Any:
    return {**data, "a": 1}


@trace_function()
async def call_all() -> None:
    await simple_function(1, 2)
    await function_with_graph_objs(SimpleDataclassModel(x=1, y="2"))
    await function_with_annotated_serializer({"b": 2})


async def test_trace_graph():
    async with trace_node(Node()) as root:
        await call_all()
        await call_all.labeled("second")

    root_links = await read.a(NodeLink, NodeLinkFilter(parent=root.node_id))
    assert len(root_links) == 2
    root_links_by_label = {link.label: link for link in root_links}
    assert "call_all" in root_links_by_label
    assert "call_all[second]" in root_links_by_label

    # TODO: test the rest...


def test_trace_sync_graph():
    @trace_function()
    def add(x: int, y: int) -> int:
        return x + y

    @trace_function()
    def mul(x: int, y: int) -> int:
        return x * y

    @trace_function()
    def do_math():
        return add(1, mul(2, 3))

    with trace_node(Node()) as root:
        do_math()

    root_links = read.s(NodeLink, NodeLinkFilter(parent=root.node_id))
    assert len(root_links) == 1


def test_trace_with_union_annotated_func():
    @trace_function()
    def some_func(
        data: Annotated[  # noqa: ARG001
            pd.DataFrame | np.ndarray, array_serializer, dataframe_serializer
        ]
    ) -> Any:
        pass

    with trace_node(Node()):
        some_func(pd.DataFrame())


async def test_traced_function_with_node_as_arg():
    @trace_function()
    def some_func(
        data: Node,  # noqa: ARG001
    ) -> None:
        pass

    async with trace_node(Node()) as root:
        inner = Node()
        some_func(inner)

    assert exists.s(Node, NodeFilter(ancestor_of=inner.node_id, node_id=root.node_id))


async def test_traced_function_do_not_save():
    @trace_function(do_not_save={"data"})
    def some_func(
        data: Node,  # noqa: ARG001
    ) -> None:
        pass

    async with trace_node(Node()):
        inner = Node()
        some_func(inner)

    assert not exists.s(Node, NodeFilter(node_id=inner.node_id))


async def test_current_node():
    some_func_current_node = None

    @trace_function()
    def some_func(
        data: Node,  # noqa: ARG001
    ) -> None:
        nonlocal some_func_current_node
        some_func_current_node = current_node()

    async with trace_node(Node()) as root:
        assert current_node() == root
        inner = Node()
        some_func(inner)

    actual_some_func_node = await read_one.a(Node, NodeFilter(parent_of=inner.node_id))
    assert some_func_current_node == actual_some_func_node
