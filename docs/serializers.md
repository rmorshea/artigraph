# Serializers

Artifacts are serialized before being stored in the database. This allows you to store
arbitrary data types in your artifacts. By default, `artigraph` will do its best to
infer how to serialize a value based on its type. However, you can also specify a
serializer explicitly.

## Built-in Serializers

-   `artigraph.serializer.json.json_serializer` - JSON
-   `artigraph.serializer.numpy.array_serializer` - 1d and 2d
    [Numpy](https://numpy.org/) Arrays
-   `artigraph.serializer.pandas.dataframe_serializer` -
    [Pandas](https://pandas.pydata.org/) DataFrames
-   `artigraph.serializer.polars.dataframe_serializer` -
    [Polars](https://pola-rs.github.io/) DataFrames
-   `artigraph.serializer.pyarrow.feather_serializer` -
    [PyArrow](https://arrow.apache.org/docs/python/index.html) Feather Files
-   `artigraph.serializer.pyarrow.parquet_serializer` -
    [PyArrow](https://arrow.apache.org/docs/python/index.html) Parquet Files

## Custom Serializers

You can create your own serializer by subclassing `artigraph.serializer.Serializer`:

```python
from typing import TypeVar

from artigraph.serializer import Serializer

T = TypeVar("T")


class CustomSerializer(Serializer[T]):

    name = "custom-serializer"  # this must be globally unique and stable across versions
    types = (SomeType,)  # the types that this serializer can handle

    def serialize(self, value: T) -> bytes:
        """serialize the value to bytes"""

    def deserialize(self, value: bytes) -> T:
        """deserialize the value from bytes"""
```
