import logging
import warnings
from typing import Any, Protocol, TypedDict, TypeVar, runtime_checkable

T = TypeVar("T")
S = TypeVar("S", bound="Serializer[Any]")

WRAPPER_VERSION = 1
SERIALIZERS_BY_TYPE: dict[type[Any], "Serializer[Any]"] = {}
SERIALIZERS_BY_NAME: dict[str, "Serializer[Any]"] = {}

logger = logging.getLogger(__name__)


def get_serialize_by_name(name: str) -> "Serializer[Any]":
    """Get a serializer by name."""
    if name not in SERIALIZERS_BY_NAME:
        msg = f"No serializer named {name!r}"
        raise ValueError(msg)
    return SERIALIZERS_BY_NAME[name]


def get_serializer_by_type(value: type[T] | T) -> "Serializer[T]":
    """Get a serializer for a value."""
    for cls in (value if isinstance(value, type) else value).mro():
        if cls in SERIALIZERS_BY_TYPE:
            return SERIALIZERS_BY_TYPE[cls]
    msg = f"No serializer exists for {value!r}"
    raise ValueError(msg)


def register_serializer(serializer: "Serializer") -> None:
    """Register a serializer.

    It's recommended that each serializer be defined and registerd in a separate module
    so that users can select which serializers they want to use by importing the module.
    Thus if a user does not import a serializer if will not be registered. This is
    important for two reasons:

    1. It allows users to avoid importing dependencies they don't need.
    2. Serializers that supprt the same type will override each other - only the last one
       registered will be used unless the user explicitly selects one.
    """
    if not isinstance(serializer, Serializer):
        msg = f"{serializer} is not of Serializer"
        raise ValueError(msg)

    if type(serializer).__module__ in serializer.name:
        warnings.warn(
            "Serializer name contains 'module.__name__' which may change between "
            "versions. Consider avoiding dynamic names",
            UserWarning,
            stacklevel=2,
        )

    if serializer.name in SERIALIZERS_BY_NAME:
        msg = f"Serializer named {serializer.name!r} already registered."
        raise ValueError(msg)
    SERIALIZERS_BY_NAME[serializer.name] = serializer

    for st in serializer.types if isinstance(serializer.types, tuple) else (serializer.types,):
        if st in SERIALIZERS_BY_TYPE:
            logger.debug("Overriding serializer for type %s with %s", st, serializer)
        SERIALIZERS_BY_TYPE[st] = serializer


@runtime_checkable
class Serializer(Protocol[T]):
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

    types: type[T] | tuple[type[T], ...]
    """The type or types of values that can be serialized."""

    def __init__(self) -> None:
        """Initialize the serializer."""

    def serialize(self, value: T, /) -> bytes:
        """Serialize a value to a string or bytes."""

    def deserialize(self, value: bytes, /) -> T:
        """Deserialize a string or bytes to a value."""


class DataWrapper(TypedDict):
    """A wrapper for serialized data."""

    wrapper_version: int
    serializer_name: int
    data_encoding: str
    data: str
