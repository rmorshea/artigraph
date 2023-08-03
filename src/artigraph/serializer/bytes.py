from artigraph.serializer import Serializer, register_serializer


class BytesSerializer(Serializer[bytes]):
    """A serializer for JSON."""

    types = (bytes,)
    name = "artigraph-bytes"

    serialize = staticmethod(lambda b: b)
    deserialize = staticmethod(lambda b: b)


bytes_serializer = register_serializer(BytesSerializer())
