import time

from artigraph.core.api.node import Node
from artigraph.core.linker import Linker, linked
from artigraph.core.model.base import GraphModel
from artigraph.core.model.dataclasses import dataclass
from artigraph.extras.networkx import create_graph


@linked()
def add(x: int, y: int) -> int:
    return x + y


@linked()
def mul(x: int, y: int) -> int:
    return x * y


@dataclass
class DidMath(GraphModel, version=1):
    result: int
    elapsed: float


@linked()
def do_math() -> DidMath:
    start = time.time()
    result = add(1, mul(2, 3))
    elapsed = time.time() - start
    return DidMath(result, elapsed)


async def test_create_graph():
    async with Linker(Node()) as root:
        assert do_math().result == 7

    graph = create_graph.s(root.node)
    assert len(graph.nodes) == 13
