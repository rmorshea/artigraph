import plotly.express as px

from artigraph.core.api.node import Node
from artigraph.core.graph.trace import trace_node
from artigraph.extras.networkx import create_graph
from artigraph.extras.plotly import figure_from_networkx_graph, figure_json_serializer
from tests.test_extras.test_networkx import do_math


def test_figure_serializer():
    fig = px.bar(x=["a", "b", "c"], y=[1, 3, 2])
    data = figure_json_serializer.serialize(fig)
    assert figure_json_serializer.deserialize(data) == fig


async def test_figure_from_networkx_graph():
    async with trace_node(Node()) as root:
        do_math()

    graph = await create_graph.a(root)
    figure_from_networkx_graph(graph)  # just check that there are no errors
