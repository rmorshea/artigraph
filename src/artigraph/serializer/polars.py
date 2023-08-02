import polars as pl

from artigraph.serializer._core import Serializer, register_serializer
from artigraph.serializer.pyarrow import ArrowSerializer, parquet_serializer


class PolarsSerializer(Serializer[pl.DataFrame]):
    """A serializer for Polars dataframes."""

    types = (pl.DataFrame,)

    def __init__(self, pyarrow_serializer: ArrowSerializer = parquet_serializer):
        self.pyarrow_serializer = pyarrow_serializer
        self.name = f"artigraph-polars-{pyarrow_serializer.name}"

    def serialize(self, value: pl.DataFrame) -> bytes:
        """Serialize a Polars dataframe."""
        return self.pyarrow_serializer.serialize(value.to_arrow())

    def deserialize(self, value: bytes) -> pl.DataFrame:
        """Deserialize a Polars dataframe."""
        return pl.from_arrow(self.pyarrow_serializer.deserialize(value))  # type: ignore


polars_serializer = register_serializer(PolarsSerializer())
"""A serializer for Polars dataframes that uses the parquet file format."""
