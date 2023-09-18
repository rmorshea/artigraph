from __future__ import annotations

from typing import Callable

import pytest
from boto3 import client

from artigraph.core.storage.base import Storage
from artigraph.core.storage.file import FileSystemStorage, temp_file_storage
from artigraph.extras.aws import S3Storage


def _make_s3_storage():
    s3_client = client("s3")
    s3_client.create_bucket(Bucket="test-bucket")
    return S3Storage("test-bucket", "test/path", s3_client=s3_client)


@pytest.mark.parametrize(
    "make_storage",
    [
        _make_s3_storage,
        lambda: temp_file_storage,
    ],
)
async def test_storage(make_storage: Callable[[], Storage]):
    """Test the S3 storage backend."""
    storage = make_storage()
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
