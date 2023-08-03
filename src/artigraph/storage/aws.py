"""S3 storage backend for Artigraph."""

import hashlib
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Callable, Iterator, TypeVar, cast

from botocore.client import BaseClient
from botocore.exceptions import ClientError
from typing_extensions import ParamSpec

from artigraph.storage.core import Storage
from artigraph.utils import run_in_thread, slugify

P = ParamSpec("P")
R = TypeVar("R")

_S3_CLIENT: ContextVar[BaseClient] = ContextVar("S3_CLIENT")


def get_s3_client() -> BaseClient:
    """Get the current S3 client."""
    try:
        return _S3_CLIENT.get()
    except LookupError:  # nocov
        msg = "No S3 client is currently set. Did you forget to use s3_client_context()?"
        raise LookupError(msg) from None


def set_s3_client(client: BaseClient) -> Callable[[], None]:
    """Set the current S3 client."""
    token = _S3_CLIENT.set(client)
    return lambda: _S3_CLIENT.reset(token)


@contextmanager
def s3_client_context(client: BaseClient) -> Iterator[BaseClient]:
    """Set the current S3 client."""
    reset = set_s3_client(client)
    try:
        yield client
    finally:
        reset()


class S3Storage(Storage):
    """S3 storage backend for Artigraph."""

    def __init__(self, bucket: str, prefix: str = "") -> None:
        """Initialize the storage backend."""
        self.name = slugify(f"artigraph-s3-{bucket}-{prefix}")
        self.bucket = bucket
        self.prefix = prefix

    async def create(self, value: bytes) -> str:
        """Create an S3 object and return is key."""
        client = get_s3_client()

        hashed_value = hashlib.sha512(value).hexdigest()
        key = f"{self.prefix}/{hashed_value}"

        # Only create the object if it doesn't already exist.
        try:
            await run_in_thread(client.head_object, Bucket=self.bucket, Key=key)
        except ClientError as error:
            if error.response["Error"]["Code"] != "404":
                raise  # nocov
            await run_in_thread(client.put_object, Bucket=self.bucket, Key=key, Body=value)

        return key

    async def read(self, key: str) -> bytes:
        """Read an S3 object."""
        client = get_s3_client()
        response = await run_in_thread(client.get_object, Bucket=self.bucket, Key=key)
        return cast(bytes, response["Body"].read())

    async def update(self, key: str, value: bytes) -> None:
        """Update an S3 object."""
        client = get_s3_client()
        await run_in_thread(client.put_object, Bucket=self.bucket, Key=key, Body=value)

    async def delete(self, key: str) -> None:
        """Delete an S3 object."""
        client = get_s3_client()
        await run_in_thread(client.delete_object, Bucket=self.bucket, Key=key)

    async def exists(self, key: str) -> bool:
        """Check if an S3 object exists."""
        client = get_s3_client()
        try:
            await run_in_thread(client.head_object, Bucket=self.bucket, Key=key)
        except ClientError as error:
            if error.response["Error"]["Code"] == "404":
                return False
            raise  # nocov
        return True
