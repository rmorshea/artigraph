from __future__ import annotations

import polars as pl

from artigraph.core.serializer.base import Serializer
from artigraph.extras.pyarrow import ArrowSerializer, parquet_serializer


class DataFrameSerializer(Serializer[pl.DataFrame]):
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


dataframe_serializer = DataFrameSerializer().register()
"""A serializer for Polars dataframes that uses the parquet file format."""
