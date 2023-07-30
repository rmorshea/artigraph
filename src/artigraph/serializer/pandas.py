import pandas as pd

from artigraph.serializer import register_serializer


class PandasSerializer:
    """A serializer for Pandas dataframes."""

    types = (pd.DataFrame, pd.Series)
    name = "artigraph.pandas"

    @staticmethod
    def serialize(value: pd.DataFrame | pd.Series) -> bytes:
        """Serialize a Pandas dataframe."""
        return value.to_parquet()

    @staticmethod
    def deserialize(value: bytes) -> pd.DataFrame | pd.Series:
        """Deserialize a Pandas dataframe."""
        return pd.read_parquet(value)


pandas_serializer = PandasSerializer()
"""A serializer for Pandas dataframes."""

register_serializer(pandas_serializer)
