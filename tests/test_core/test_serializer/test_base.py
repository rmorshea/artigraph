import pytest

from artigraph.core.serializer.base import Serializer


class IntSerializer(Serializer[int]):
    types = (int,)
    name = "artigraph-int"

    def serialize(self, value: int) -> bytes:
        return str(value).encode()

    def deserialize(self, value: bytes) -> int:
        return int(value.decode())


int_serializer = IntSerializer().register()


def test_cannot_register_serializer_with_same_name():
    """Test that storage backends cannot be registered with the same name."""
    with pytest.raises(ValueError, match=r"Serializer named 'artigraph-int' already registered"):
        IntSerializer().register()
