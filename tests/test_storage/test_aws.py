import pytest
from boto3 import client

from artigraph.storage.aws import S3Storage, s3_client_context


@pytest.fixture(autouse=True)
def s3_bucket():
    with s3_client_context(client("s3")) as s3:
        s3.create_bucket(Bucket="test-bucket")
        yield


async def test_s3_storage():
    """Test the S3 storage backend."""
    storage = S3Storage("test-bucket", "test/path")
    key = await storage.create(b"Hello, world!")
    assert await storage.exists(key)
    assert await storage.read(key) == b"Hello, world!"
    await storage.update(key, b"Goodbye, world!")
    assert await storage.read(key) == b"Goodbye, world!"
    await storage.delete(key)
    assert not await storage.exists(key)
