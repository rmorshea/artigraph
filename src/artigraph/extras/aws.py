"""S3 storage backend for Artigraph."""
from __future__ import annotations

import hashlib
from typing import cast

from botocore.client import BaseClient
from botocore.exceptions import ClientError

from artigraph.core.storage.base import Storage
from artigraph.core.utils.misc import run_in_thread, slugify


class S3Storage(Storage):
    """S3 storage backend for Artigraph.

    Parameters:
        bucket: The name of the S3 bucket.
        prefix: The prefix to use for all S3 keys.
        s3_client: The S3 client to use.
    """

    def __init__(self, bucket: str, prefix: str = "", *, s3_client: BaseClient) -> None:
        self.name = slugify(f"artigraph-s3-{bucket}-{prefix}")
        self.bucket = bucket
        self.prefix = prefix
        self.client = s3_client

    async def create(self, value: bytes) -> str:
        """Create an S3 object and return is key."""
        hashed_value = hashlib.sha512(value).hexdigest()
        key = f"{self.prefix}/{hashed_value}"

        # Only create the object if it doesn't already exist.
        try:
            await run_in_thread(self.client.head_object, Bucket=self.bucket, Key=key)
        except ClientError as error:
            if error.response["Error"]["Code"] != "404":
                raise  # nocov
            await run_in_thread(self.client.put_object, Bucket=self.bucket, Key=key, Body=value)

        return key

    async def read(self, key: str) -> bytes:
        """Read an S3 object."""
        response = await run_in_thread(self.client.get_object, Bucket=self.bucket, Key=key)
        return cast(bytes, response["Body"].read())

    async def update(self, key: str, value: bytes) -> None:
        """Update an S3 object."""
        await run_in_thread(self.client.put_object, Bucket=self.bucket, Key=key, Body=value)

    async def delete(self, key: str) -> None:
        """Delete an S3 object."""
        await run_in_thread(self.client.delete_object, Bucket=self.bucket, Key=key)

    async def exists(self, key: str) -> bool:
        """Check if an S3 object exists."""
        try:
            await run_in_thread(self.client.head_object, Bucket=self.bucket, Key=key)
        except ClientError as error:
            if error.response["Error"]["Code"] == "404":
                return False
            raise  # nocov
        return True
