# Storage

Artifacts can be stored in a variety of locations. By default, artifacts are stored in
the database itself. However, you can also store artifacts in a local filesystem or in
AWS S3.

## Built-in Storage Backends

All built-in serializers can be found under `artigraph.storage`:

| Storage                                                            | Description                          |
| ------------------------------------------------------------------ | ------------------------------------ |
| [file.FileSystemStorage][artigraph.storage.file.FileSystemStorage] | Local filesystem                     |
| [aws.S3Storage](#s3-storage)                                       | AWS [S3](https://aws.amazon.com/s3/) |

## Custom Storage Backends

You can create your own storage backend by subclassing `artigraph.storage.Storage`:

```python
from artigraph.storage import Storage


class CustomStorage(Storage):

    def __init__(self):
        # This must be GLOBALLY unique and stable across versions!
        self.name = "custom-storage"

    async def create(self, data: bytes) -> str:
        """Create the artifact data and return its location."""

    async def read(self, location: str) -> bytes:
        """Read artifact data from the given location."""

    async def update(self, location: str, data: bytes) -> None:
        """Update artifact data at the given location."""

    async def delete(self, location: str) -> None:
        """Delete artifact data at the given location."""

    async def exists(self, location: str) -> bool:
        """Check if artifact data exists at the given location."""


CustomStorage().regiser()
```
