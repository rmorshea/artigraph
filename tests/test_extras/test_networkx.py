from artigraph.core.api.node import Node
from artigraph.core.graph.trace import trace_function, trace_node
from artigraph.extras.networkx import create_graph


@trace_function()
def add(x: int, y: int) -> int:
    return x + y


@trace_function()
def mul(x: int, y: int) -> int:
    return x * y


@trace_function()
def do_math():
    return add(1, mul(2, 3))


async def test_create_graph():
    async with trace_node(Node()) as root:
        assert do_math() == 7

    graph = create_graph.s(root)
    assert len(graph.nodes) == 11
