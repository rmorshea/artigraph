import pandas as pd

from artigraph.extras.pandas import dataframe_serializer


def test_serialize_deserialize_pandas_dataframe():
    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    serialized = dataframe_serializer.serialize(df)
    assert dataframe_serializer.deserialize(serialized).equals(df)
