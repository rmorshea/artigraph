import json
from typing import Any

from artigraph.serializer import Serializer


class JsonSerializer(Serializer[Any]):
    """A serializer for JSON."""

    types = (object,)
    name = "artigraph-json"

    def serialize(self, value: Any) -> bytes:
        """Serialize a value."""
        return json.dumps(value).encode("utf-8")

    def deserialize(self, value: bytes) -> Any:
        """Deserialize a value."""
        return json.loads(value.decode("utf-8"))


json_serializer = JsonSerializer().register()
"""A serializer for JSON."""
