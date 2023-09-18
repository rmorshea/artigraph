import polars as pl

from artigraph.extras.polars import dataframe_serializer


def test_serialize_deserialize_polars_dataframe():
    df = pl.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    serialized = dataframe_serializer.serialize(df)
    assert dataframe_serializer.deserialize(serialized).frame_equal(df)
