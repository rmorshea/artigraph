from __future__ import annotations

from io import BytesIO

import pandas as pd

from artigraph.core.serializer.base import Serializer


class DataFrameSerializer(Serializer[pd.DataFrame]):
    """A serializer for Pandas dataframes."""

    types = (pd.DataFrame,)
    name = "artigraph-pandas"

    @staticmethod
    def serialize(value: pd.DataFrame) -> bytes:
        """Serialize a Pandas dataframe."""
        return value.to_parquet()

    @staticmethod
    def deserialize(value: bytes) -> pd.DataFrame:
        """Deserialize a Pandas dataframe."""
        return pd.read_parquet(BytesIO(value))


dataframe_serializer = DataFrameSerializer().register()
"""A serializer for Pandas dataframes."""
