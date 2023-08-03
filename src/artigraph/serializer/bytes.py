from artigraph.serializer import Serializer, register_serializer


class BytesSerializer(Serializer[bytes]):
    """A serializer for JSON."""

    types = (bytes,)
    name = "artigraph-bytes"

    serialize = staticmethod(lambda b: b)  # type: ignore
    deserialize = staticmethod(lambda b: b)  # type: ignore


bytes_serializer = register_serializer(BytesSerializer())
