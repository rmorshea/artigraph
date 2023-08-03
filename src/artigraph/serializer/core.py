import logging
from abc import ABC, abstractmethod
from inspect import isclass
from typing import Any, Generic, Sequence, TypedDict, TypeVar

from typing_extensions import Self

T = TypeVar("T")
S = TypeVar("S", bound="Serializer[Any]")

WRAPPER_VERSION = 1
SERIALIZERS_BY_TYPE: dict[type[Any], "Serializer[Any]"] = {}
SERIALIZERS_BY_NAME: dict[str, "Serializer[Any]"] = {}

logger = logging.getLogger(__name__)


def get_serialize_by_name(name: str) -> "Serializer[Any]":
    """Get a serializer by name."""
    if name not in SERIALIZERS_BY_NAME:  # nocov
        msg = f"No serializer named {name!r}"
        raise ValueError(msg)
    return SERIALIZERS_BY_NAME[name]


def get_serializer_by_type(value: type[T] | T) -> "Serializer[T]":
    """Get a serializer for a value."""
    for cls in (value if isclass(value) else type(value)).mro():
        if cls in SERIALIZERS_BY_TYPE:
            return SERIALIZERS_BY_TYPE[cls]
    msg = f"No serializer exists for {value!r}"  # nocov
    raise ValueError(msg)  # nocov


class Serializer(ABC, Generic[T]):
    """A type of artifact that can be serialized to a string or bytes."""

    name: str
    """A globally unique name for this serializer.

    This will typically be of the form "library_name.SerializerName". You should avoid
    using dynamic values like `__name__` or `__qualname__` as these may change between
    versions of the library or if you move the class to a different module.

    The serializer name will be used to recover this class from a when deserializing
    artifacts so it must not change between versions of the library. If you need to
    change the name, you should create and register a subclass with the new name and
    deprecate the old one.
    """

    types: Sequence[type[T]]
    """The type or types of values that can be serialized."""

    def register(self) -> Self:
        """Register a serializer.

        It's recommended that each serializer be defined and registerd in a separate module
        so that users can select which serializers they want to use by importing the module.
        Thus if a user does not import a serializer if will not be registered. This is
        important for two reasons:

        1. It allows users to avoid importing dependencies they don't need.
        2. Serializers that supprt the same type will override each other - only the last one
        registered will be used unless the user explicitly selects one.
        """
        if not isinstance(self, Serializer):  # nocov
            msg = f"{self} is not of Serializer"
            raise ValueError(msg)

        if self.name in SERIALIZERS_BY_NAME:
            msg = f"Serializer named {self.name!r} already registered."
            raise ValueError(msg)
        SERIALIZERS_BY_NAME[self.name] = self

        for serializable_type in self.types:
            if serializable_type not in SERIALIZERS_BY_TYPE:
                SERIALIZERS_BY_TYPE[serializable_type] = self
            else:
                logger.debug(
                    "Did not register %s for %s - %s already exists",
                    self,
                    serializable_type,
                    SERIALIZERS_BY_TYPE[serializable_type],
                )

        return self

    @abstractmethod
    def serialize(self, value: T, /) -> bytes:
        """Serialize a value to a string or bytes."""
        raise NotImplementedError()  # nocov

    @abstractmethod
    def deserialize(self, value: bytes, /) -> T:
        """Deserialize a string or bytes to a value."""
        raise NotImplementedError()  # nocov


class DataWrapper(TypedDict):
    """A wrapper for serialized data."""

    wrapper_version: int
    serializer_name: int
    data_encoding: str
    data: str


class StringSerializer(Serializer[str]):
    """A serializer for JSON."""

    types = (str,)
    name = "artigraph-string"

    serialize = staticmethod(str.encode)  # type: ignore
    deserialize = staticmethod(bytes.decode)  # type: ignore


StringSerializer().register()


class BytesSerializer(Serializer[bytes]):
    """A serializer for JSON."""

    types = (bytes,)
    name = "artigraph-bytes"

    serialize = staticmethod(lambda b: b)  # type: ignore
    deserialize = staticmethod(lambda b: b)  # type: ignore


BytesSerializer().register()
