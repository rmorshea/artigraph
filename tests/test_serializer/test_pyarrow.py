import pyarrow as pa

from artigraph.serializer.pyarrow import feather_serializer, parquet_serializer


def test_feather_serializer():
    table = pa.Table.from_pydict({"a": [1, 2, 3], "b": [4, 5, 6]})
    serialized = feather_serializer.serialize(table)
    assert feather_serializer.deserialize(serialized).equals(table)


def test_parquet_serializer():
    table = pa.Table.from_pydict({"a": [1, 2, 3], "b": [4, 5, 6]})
    serialized = parquet_serializer.serialize(table)
    assert parquet_serializer.deserialize(serialized).equals(table)


def test_only_table_is_supported():
    try:
        feather_serializer.serialize(pa.array([1, 2, 3]))
    except TypeError:
        pass
    else:
        msg = "Should not be able to serialize a non-table."
        raise AssertionError(msg)
