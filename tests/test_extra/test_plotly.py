import plotly.express as px

from artigraph.extra.plotly import figure_json_serializer


def test_figure_serializer():
    fig = px.bar(x=["a", "b", "c"], y=[1, 3, 2])
    data = figure_json_serializer.serialize(fig)
    assert figure_json_serializer.deserialize(data) == fig
