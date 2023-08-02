import polars as pl

from artigraph.serializer.polars import polars_serializer


def test_serialize_deserialize_polars_dataframe():
    df = pl.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    serialized = polars_serializer.serialize(df)
    assert polars_serializer.deserialize(serialized).frame_equal(df)