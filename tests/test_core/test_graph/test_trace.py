from dataclasses import replace
from typing import Annotated, Any

from artigraph.core.api.filter import NodeLinkFilter
from artigraph.core.api.funcs import read
from artigraph.core.api.link import NodeLink
from artigraph.core.api.node import Node
from artigraph.core.graph.trace import graph_tracer, trace_graph
from artigraph.core.serializer.json import json_sorted_serializer
from tests.common.model import SimpleDataclassModel


@graph_tracer()
async def simple_function(x: int, y: int) -> int:
    return x + y


@graph_tracer()
async def function_with_graph_objs(obj: SimpleDataclassModel) -> SimpleDataclassModel:
    return replace(obj, x=obj.x + 1)


@graph_tracer()
async def function_with_annotated_serializer(data: Annotated[Any, json_sorted_serializer]) -> None:
    return {**data, "a": 1}


@graph_tracer()
async def call_all() -> None:
    await simple_function(1, 2)
    await function_with_graph_objs(SimpleDataclassModel(x=1, y=2))
    await function_with_annotated_serializer({"b": 2})


async def test_trace_graph():
    async with trace_graph(Node()) as root:
        await call_all()
        await call_all.labeled("second")

    root_links = await read.a(NodeLink, NodeLinkFilter(parent=root.node_id))
    assert len(root_links) == 2
    root_links_by_label = {link.label: link for link in root_links}
    assert "call_all" in root_links_by_label
    assert "call_all[second]" in root_links_by_label

    # TODO: test the rest...
    raise AssertionError()
