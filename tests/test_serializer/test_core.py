import pytest

from artigraph.serializer import Serializer, get_serializer_by_type


class IntSerializer(Serializer[int]):
    types = (int,)
    name = "artigraph-int"

    def serialize(self, value: int) -> bytes:
        return value.to_bytes()

    def deserialize(self, value: bytes) -> int:
        return int.from_bytes(value)


int_serializer = IntSerializer().register()


def test_cannot_register_serializer_with_same_name():
    """Test that storage backends cannot be registered with the same name."""
    with pytest.raises(ValueError, match=r"Serializer named 'artigraph-int' already registered"):
        IntSerializer().register()


def test_get_serilizer_by_type():
    """Test that we can get a serializer by type."""

    class SpecialInt(int):
        is_super_special = True

    assert get_serializer_by_type(SpecialInt) is int_serializer
