# Serializers

Artifacts are serialized before being stored in the database. This allows you to store
arbitrary data types in your artifacts. By default, `artigraph` will do its best to
infer how to serialize a value based on its type. However, you can also specify a
serializer explicitly.

## Built-in Serializers

All built-in serializers can be found under `artigraph.serializer`:

| Serializer                      | Description                                                              |
| ------------------------------- | ------------------------------------------------------------------------ |
| `datetime.datetime_serializer`  | Date Times                                                               |
| `datetime.timedelta_serializer` | Time Deltas                                                              |
| `json.json_serializer`          | JSON                                                                     |
| `json.json_sorted_serializer`   | JSON with sorted keys                                                    |
| `numpy.array_serializer`        | 1d and 2d [Numpy](https://numpy.org/) Arrays                             |
| `pandas.dataframe_serializer`   | [Pandas](https://pandas.pydata.org/) DataFrames                          |
| `polars.dataframe_serializer`   | [Polars](https://pola-rs.github.io/) DataFrames                          |
| `pyarrow.feather_serializer`    | [PyArrow](https://arrow.apache.org/docs/python/index.html) Feather Files |
| `pyarrow.parquet_serializer`    | [PyArrow](https://arrow.apache.org/docs/python/index.html) Parquet Files |

## Custom Serializers

You can create your own serializer by subclassing `artigraph.serializer.Serializer`:

```python
from typing import TypeVar

from artigraph.serializer import Serializer

T = TypeVar("T")


class CustomSerializer(Serializer[T]):

    def __init__(self):
        # This must be GLOBALLY unique and stable across versions!
        self.name = "custom-serializer"

    def serialize(self, value: T) -> bytes:
        """serialize the value to bytes"""

    def deserialize(self, value: bytes) -> T:
        """deserialize the value from bytes"""


CustomSerializer().register()
```
