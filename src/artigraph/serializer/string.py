from artigraph.serializer import Serializer, register_serializer


class StringSerializer(Serializer[str]):
    """A serializer for JSON."""

    types = (str,)
    name = "artigraph-string"

    serialize = staticmethod(str.encode)  # type: ignore
    deserialize = staticmethod(bytes.decode)  # type: ignore


string_serializer = register_serializer(StringSerializer())
"""A serializer for strings."""
