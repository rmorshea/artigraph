from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import TypeVar

from typing_extensions import Self

B = TypeVar("B", bound="str | bytes")
S = TypeVar("S", bound="Storage")

WRAPPER_VERSION = 1
STORAGE_BY_NAME: dict[str, Storage] = {}

logger = logging.getLogger(__name__)


def get_storage_by_name(name: str) -> Storage:
    if name not in STORAGE_BY_NAME:  # nocov
        msg = f"No storage named {name!r} exists."
        raise ValueError(msg)
    return STORAGE_BY_NAME[name]


class Storage(ABC):
    """A storage backend for artifacts."""

    name: str
    """A globally unique name for this storage.

    This will typically be of the form "library_name-storage_name". You should avoid
    using dynamic values like `__name__` or `__qualname__` as these may change between
    versions of the library or if you move the class to a different module.

    The storage name will be used to recover this class when loading data from records.
    It must not change between versions of the library. If you need to change the name,
    you should create and register a subclass with the new name and deprecate the old
    one.
    """

    def register(self) -> Self:
        """Register a storage backend.

        It's recommended that each storage backend be defined and registerd in a separate
        module so that users can select which storage they want to use by importing the module.
        Thus, if a user does not import a storage backend it will not be registered. This is
        important because some storage backends may have dependencies that are not installed.
        """
        if self.name in STORAGE_BY_NAME:
            msg = (
                f"Serializer named {self.name!r} already "
                f"registered as {STORAGE_BY_NAME[self.name]!r}"
            )
            raise ValueError(msg)

        STORAGE_BY_NAME[self.name] = self

        return self

    @abstractmethod
    async def create(self, data: bytes, /) -> str:
        """Create the artifact data and return its location."""
        ...

    @abstractmethod
    async def read(self, location: str, /) -> bytes:
        """Read artifact data from the given location."""
        ...

    @abstractmethod
    async def update(self, location: str, data: bytes, /) -> None:
        """Update artifact data at the given location."""
        ...

    @abstractmethod
    async def delete(self, location: str, /) -> None:
        """Delete artifact data at the given location."""
        ...

    @abstractmethod
    async def exists(self, location: str, /) -> bool:
        """Check if artifact data exists at the given location."""
        ...
