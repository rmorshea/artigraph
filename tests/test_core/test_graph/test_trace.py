from dataclasses import replace
from typing import Annotated, Any

import numpy as np
import pandas as pd

from artigraph.core.api.filter import NodeLinkFilter
from artigraph.core.api.funcs import read
from artigraph.core.api.link import NodeLink
from artigraph.core.api.node import Node
from artigraph.core.graph.trace import start_trace, trace_function
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
    async with start_trace(Node()) as root:
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

    with start_trace(Node()) as root:
        do_math()

    root_links = read.s(NodeLink, NodeLinkFilter(parent=root.node_id))
    assert len(root_links) == 1


def test_trace_with_union_annotated_func():
    @trace_function()
    def add_one_second(
        data: Annotated[pd.DataFrame | np.ndarray, array_serializer, dataframe_serializer]
    ) -> Any:
        pass

    with start_trace(Node()):
        add_one_second(pd.DataFrame())
