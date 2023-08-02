import pandas as pd

from artigraph.serializer.pandas import pandas_serializer


def test_serialize_deserialize_pandas_dataframe():
    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    serialized = pandas_serializer.serialize(df)
    assert pandas_serializer.deserialize(serialized).equals(df)
