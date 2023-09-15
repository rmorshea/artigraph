from __future__ import annotations

from io import BytesIO
from typing import Literal

import pyarrow as pa
from pyarrow import feather, parquet

from artigraph.core.serializer.base import Serializer


class ArrowSerializer(Serializer[pa.Table]):
    """A serializer for PyArrow tables."""

    types = (pa.Table,)

    def __init__(self, file_format: Literal["feather", "parquet"]):
        self.file_format = file_format
        self.name = f"artigraph-pyarrow-{file_format}"

    def serialize(self, value: pa.Table) -> bytes:
        """Serialize a PyArrow table."""
        if not isinstance(value, pa.Table):
            msg = f"Expected a PyArrow table, got {type(value)}"
            raise TypeError(msg)
        method = getattr(self, f"serialize_{self.file_format}")
        return method(value)

    def deserialize(self, value: bytes) -> pa.Table:
        """Deserialize a PyArrow table."""
        method = getattr(self, f"deserialize_{self.file_format}")
        return method(value)

    def serialize_parquet(self, value: pa.Table) -> bytes:
        """Serialize a PyArrow table to Parquet."""
        buffer = BytesIO()
        parquet.write_table(value, buffer)
        return buffer.getvalue()

    def deserialize_parquet(self, value: bytes) -> pa.Table:
        """Deserialize a PyArrow table from Parquet."""
        buffer = BytesIO(value)
        return parquet.read_table(buffer)

    def serialize_feather(self, value: pa.Table) -> bytes:
        """Serialize a PyArrow table to Feather."""
        buffer = BytesIO()
        feather.write_feather(value, buffer)
        return buffer.getvalue()

    def deserialize_feather(self, value: bytes) -> pa.Table:
        """Deserialize a PyArrow table from Feather."""
        buffer = BytesIO(value)
        return feather.read_table(buffer)


feather_serializer = ArrowSerializer("feather").register()
"""A serializer for PyArrow tables using Feather."""

parquet_serializer = ArrowSerializer("parquet").register()
"""A serializer for PyArrow tables using Parquet."""
