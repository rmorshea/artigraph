"""S3 storage backend for Artigraph."""

import hashlib
from contextvars import ContextVar
from email import contentmanager
from functools import partial
from typing import Callable, TypeVar, cast

import boto3
from anyio import from_thread
from botocore.exceptions import ClientError
from typing_extensions import ParamSpec

from artigraph.storage._core import register_storage

P = ParamSpec("P")
R = TypeVar("R")

_S3_CLIENT: ContextVar[boto3.client] = ContextVar("S3_CLIENT")


def set_s3_client(client: boto3.client) -> None:
    """Set the current S3 client."""
    token = _S3_CLIENT.set(client)
    return lambda: _S3_CLIENT.reset(token)


@contentmanager
def s3_context(client: boto3.client) -> ContextVar[boto3.client]:
    """Set the current S3 client."""
    reset = set_s3_client(client)
    try:
        yield
    finally:
        reset()


class S3Storage:
    """S3 storage backend for Artigraph."""

    def __init__(self, bucket: str, prefix: str) -> None:
        """Initialize the storage backend."""
        self.name = f"artigraph.s3.{bucket}.{prefix}"
        self.bucket = bucket
        self.prefix = prefix
        register_storage(self)

    async def create(self, value: bytes) -> str:
        """Create an S3 object and return is key."""
        client = _S3_CLIENT.get()

        hashed_value = hashlib.sha512(value).hexdigest()
        key = f"{self.prefix}/{hashed_value}"

        # Only create the object if it doesn't already exist.
        try:
            await _run_in_thread(client.head_object, Bucket=self.bucket, Key=key)
        except ClientError as error:
            if error.response["Error"]["Code"] != "NoSuchKey":
                raise
            await _run_in_thread(client.put_object, Bucket=self.bucket, Key=key, Body=value)

        return key

    async def read(self, key: str) -> bytes:
        """Read an S3 object."""
        client = _S3_CLIENT.get()
        response = await _run_in_thread(client.get_object, Bucket=self.bucket, Key=key)
        return cast(bytes, response["Body"].read())

    async def update(self, key: str, value: bytes) -> None:
        """Update an S3 object."""
        client = _S3_CLIENT.get()
        await _run_in_thread(client.put_object, Bucket=self.bucket, Key=key, Body=value)

    async def delete(self, key: str) -> None:
        """Delete an S3 object."""
        client = _S3_CLIENT.get()
        await _run_in_thread(client.delete_object, Bucket=self.bucket, Key=key)


async def _run_in_thread(func: Callable[P, R], /, *args: P.args, **kwargs: P.kwargs) -> R:
    return await from_thread.run(partial(func, *args, **kwargs))
