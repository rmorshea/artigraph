import polars as pl
import pyarrow as pa

from artigraph.serializer._core import register_serializer


class PolarsSerializer:
    """A serializer for Polars dataframes."""

    types = (pl.DataFrame, pl.Series)
    name = "artigraph.polars"

    @staticmethod
    def serialize(value: pl.DataFrame | pl.Series) -> bytes:
        """Serialize a Polars dataframe."""
        pa_value = value.to_arrow()
        return pa_value.serialize().to_buffer().to_pybytes()

    @staticmethod
    def deserialize(value: bytes) -> pl.DataFrame | pl.Series:
        """Deserialize a Polars dataframe."""
        pa_value = pa.deserialize(value)
        return pl.from_arrow(pa_value)


polars_serializer = PolarsSerializer()
"""A serializer for Polars dataframes."""

register_serializer(polars_serializer)
