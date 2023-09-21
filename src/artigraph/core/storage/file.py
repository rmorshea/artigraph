from __future__ import annotations

import atexit
from pathlib import Path
from tempfile import TemporaryDirectory
from uuid import uuid4

from artigraph.core.storage.base import Storage
from artigraph.core.utils.misc import slugify


class FileSystemStorage(Storage):
    """A storage backend that saves artifacts to the filesystem.

    Parameters:
        directory: The directory to save artifacts to.
        name: The name of the storage backend.
    """

    def __init__(self, directory: str | Path, name: str = "") -> None:
        self.dir = Path(directory)
        self.name = slugify(f"artigraph-file-system-{name or self.dir}")

    async def create(self, data: bytes) -> str:
        """Create an artifact in the filesystem and return its location"""
        key = uuid4().hex
        path = self.dir / key
        path.write_bytes(data)
        return key

    async def read(self, key: str) -> bytes:
        """Read an artifact from the filesystem."""
        path = self.dir / key
        return path.read_bytes()

    async def update(self, key: str, data: bytes) -> None:
        """Update an artifact in the filesystem."""
        path = self.dir / key
        path.write_bytes(data)

    async def delete(self, key: str) -> None:
        """Delete an artifact from the filesystem."""
        path = self.dir / key
        path.unlink()

    async def exists(self, key: str) -> bool:
        """Check if an artifact exists in the filesystem."""
        path = self.dir / key
        return path.exists()


temp_dir = TemporaryDirectory()
temp_file_storage = FileSystemStorage(temp_dir.name).register()
"""A temporary file storage backend (best for testing)."""

atexit.register(temp_dir.cleanup)
