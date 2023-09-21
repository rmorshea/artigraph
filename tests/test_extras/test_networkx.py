import time

from artigraph.core.api.node import Node
from artigraph.core.graph.trace import trace_function, trace_node
from artigraph.core.model.base import GraphModel
from artigraph.core.model.dataclasses import dataclass
from artigraph.extras.networkx import create_graph


@trace_function()
def add(x: int, y: int) -> int:
    return x + y


@trace_function()
def mul(x: int, y: int) -> int:
    return x * y


@dataclass
class DidMath(GraphModel, version=1):
    result: int
    elapsed: float


@trace_function()
def do_math() -> DidMath:
    start = time.time()
    result = add(1, mul(2, 3))
    elapsed = time.time() - start
    return DidMath(result, elapsed)


async def test_create_graph():
    async with trace_node(Node()) as root:
        assert do_math().result == 7

    graph = create_graph.s(root)
    assert len(graph.nodes) == 13
