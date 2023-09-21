from __future__ import annotations

import json
from typing import Any

from artigraph.core.serializer.base import Serializer


class JsonSerializer(Serializer[Any]):
    """A serializer for JSON."""

    types = (object,)

    def __init__(self, *, sort_keys: bool = False) -> None:
        self.name = f"artigraph-json-{'sorted' if sort_keys else 'unsorted'}"

    def serialize(self, value: Any) -> bytes:
        """Serialize a value."""
        return json.dumps(value, separators=(",", ":"), allow_nan=False).encode("utf-8")

    def deserialize(self, value: bytes) -> Any:
        """Deserialize a value."""
        return json.loads(value.decode("utf-8"))


json_serializer = JsonSerializer().register()
"""A serializer for JSON."""

json_sorted_serializer = JsonSerializer(sort_keys=True).register()
"""A serializer for JSON with sorted keys"""
