import pyarrow as pa
import pytest

from artigraph.extras.pyarrow import feather_serializer, parquet_serializer


def test_feather_serializer():
    table = pa.Table.from_pydict({"a": [1, 2, 3], "b": [4, 5, 6]})
    serialized = feather_serializer.serialize(table)
    assert feather_serializer.deserialize(serialized).equals(table)


def test_parquet_serializer():
    table = pa.Table.from_pydict({"a": [1, 2, 3], "b": [4, 5, 6]})
    serialized = parquet_serializer.serialize(table)
    assert parquet_serializer.deserialize(serialized).equals(table)


def test_only_table_is_supported():
    with pytest.raises(TypeError, match=r"Expected a PyArrow table, got"):
        feather_serializer.serialize(pa.array([1, 2, 3]))
