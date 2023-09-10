from artigraph.core.serializer.json import json_serializer


def test_json_serializer():
    """Test the JSON serializer."""
    value = {"hello": "world"}
    serialized = json_serializer.serialize(value)
    assert json_serializer.deserialize(serialized) == value
