from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Any, Generic, Sequence, TypeVar

from typing_extensions import Self

T = TypeVar("T")
S = TypeVar("S", bound="Serializer[Any]")

WRAPPER_VERSION = 1
SERIALIZERS_BY_NAME: dict[str, Serializer[Any]] = {}
SERIALIZERS_BY_TYPE: dict[type[Any], Sequence[Serializer[Any]]] = {}

logger = logging.getLogger(__name__)


def get_serializer_by_name(name: str) -> Serializer[Any]:
    """Get a serializer by name."""
    if name not in SERIALIZERS_BY_NAME:  # nocov
        msg = f"No serializer named {name!r}"
        raise ValueError(msg)
    return SERIALIZERS_BY_NAME[name]


def get_serializer_by_type(cls: type[T]) -> Sequence[Serializer[T]]:
    """Get a serializer by type."""
    for c in cls.mro():
        if c in SERIALIZERS_BY_TYPE:
            return SERIALIZERS_BY_TYPE[c]
    msg = f"No serializer for type {cls!r}"  # nocov
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

    types: tuple[type[T], ...]
    """The types of values this serializer supports."""

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

        for t in self.types:
            SERIALIZERS_BY_TYPE[t] = (*SERIALIZERS_BY_TYPE.get(t, ()), self)

        return self

    @abstractmethod
    def serialize(self, value: T, /) -> bytes:
        """Serialize a value to a string or bytes."""
        raise NotImplementedError()  # nocov

    @abstractmethod
    def deserialize(self, value: bytes, /) -> T:
        """Deserialize a string or bytes to a value."""
        raise NotImplementedError()  # nocov
