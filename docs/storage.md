# Storage

The data from [artifacts](./building-blocks.md#artifacts) can be stored in a variety of
locations. By default, artifact data is stored in the database itself. However, you can
specify a storage backend to save it elsewhere. You can use one of the
[built-in storage backends](#built-in-storage-backends) or you can
[create your own](#custom-storage-backends).

## Built-in Storage Backends

| Storage                                                                              | Description                          |
| ------------------------------------------------------------------------------------ | ------------------------------------ |
| [core.storage.file.FileSystemStorage][artigraph.core.storage.file.FileSystemStorage] | Local filesystem                     |
| [extra.storage.aws.S3Storage][artigraph.extras.aws.S3Storage]                        | AWS [S3](https://aws.amazon.com/s3/) |

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
