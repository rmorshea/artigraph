from artigraph.core.api.node import Node
from artigraph.core.graph.trace import start_trace, trace_function
from artigraph.extras.networkx import create_graph


async def test_create_networkx_graph():
    @trace_function()
    def add(x: int, y: int) -> int:
        return x + y

    @trace_function()
    def mul(x: int, y: int) -> int:
        return x * y

    @trace_function()
    def do_math():
        return add(1, mul(2, 3))

    async with start_trace(Node()) as root:
        do_math()

    graph = create_graph.s(root)
    assert len(graph.nodes) == 11
