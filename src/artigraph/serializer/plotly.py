from plotly import io as plotly_io
from plotly.graph_objs import Figure

from artigraph import Serializer


class FigureJsonSerializer(Serializer):
    """Serialize a plotly figure"""

    name = "artigraph-plotly-figure-json"

    def serialize(self, figure: Figure) -> bytes:
        result = plotly_io.to_json(figure)
        if result is None:  # no cov
            msg = "Plotly failed to serialize the figure - this is likely an issue with Plotly"
            raise RuntimeError(msg)
        return result.encode()

    def deserialize(self, data: bytes) -> Figure:
        return plotly_io.from_json(data.decode())


figure_json_serializer = FigureJsonSerializer().register()
"""Serialize a plotly figure"""
