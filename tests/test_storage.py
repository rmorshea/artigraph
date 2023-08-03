from contextlib import ExitStack, contextmanager
from typing import ContextManager

import pytest
from boto3 import client

from artigraph.storage import Storage
from artigraph.storage.aws import S3Storage, s3_client_context
from artigraph.storage.file import FileSystemStorage, temp_file_storage


@contextmanager
def setup_s3_bucket():
    with s3_client_context(client("s3")) as s3:
        s3.create_bucket(Bucket="test-bucket")
        yield


@pytest.mark.parametrize(
    "storage, setup_storage",
    [
        (S3Storage("test-bucket", "test/path"), setup_s3_bucket()),
        (temp_file_storage, None),
    ],
)
async def test_storage(storage: Storage, setup_storage: ContextManager | None):
    """Test the S3 storage backend."""
    with ExitStack() as stack:
        if setup_storage is not None:
            stack.enter_context(setup_storage)
        key = await storage.create(b"Hello, world!")
        assert await storage.exists(key)
        assert await storage.read(key) == b"Hello, world!"
        await storage.update(key, b"Goodbye, world!")
        assert await storage.read(key) == b"Goodbye, world!"
        await storage.delete(key)
        assert not await storage.exists(key)


def test_cannot_register_storage_with_same_name():
    """Test that storage backends cannot be registered with the same name."""
    FileSystemStorage("test").register()
    with pytest.raises(ValueError):
        FileSystemStorage("test").register()
