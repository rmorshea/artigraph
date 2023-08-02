from io import BytesIO

import pandas as pd

from artigraph.serializer import Serializer, register_serializer


class PandasSerializer(Serializer[pd.DataFrame]):
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


pandas_serializer = register_serializer(PandasSerializer())
"""A serializer for Pandas dataframes."""
