# Serializers

Artifacts are serialized before being stored in the database. This allows you to store
arbitrary data types in your artifacts. By default, `artigraph` will do its best to
infer how to serialize a value based on its type. However, you can also specify a
serializer explicitly.

## Built-in Serializers

| Serializer                                                                                                 | Description                                                              |
| ---------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------ |
| [core.serializer.datetime.datetime_serializer][artigraph.datetime_serializer]                              | Date Times                                                               |
| [core.serializer.json.json_serializer][artigraph.json_serializer]                                          | JSON                                                                     |
| [core.serializer.json.json_sorted_serializer][artigraph.json_sorted_serializer]                            | JSON with sorted keys                                                    |
| [extra.serializer.numpy.array_serializer][artigraph.extra.serializer.numpy.array_serializer]               | 1d and 2d [Numpy](https://numpy.org/) Arrays                             |
| [extra.serializer.pandas.dataframe_serializer][artigraph.extra.serializer.pandas.dataframe_serializer]     | [Pandas](https://pandas.pydata.org/) DataFrames                          |
| [extra.serializer.plotly.figure_json_serializer][artigraph.extra.serializer.plotly.figure_json_serializer] | [Plotly](https://plotly.com/python/) Figures                             |
| [extra.serializer.polars.dataframe_serializer][artigraph.extra.serializer.polars.dataframe_serializer]     | [Polars](https://pola-rs.github.io/) DataFrames                          |
| [extra.serializer.pyarrow.feather_serializer][artigraph.extra.serializer.pyarrow.feather_serializer]       | [PyArrow](https://arrow.apache.org/docs/python/index.html) Feather Files |
| [extra.serializer.pyarrow.parquet_serializer][artigraph.extra.serializer.pyarrow.parquet_serializer]       | [PyArrow](https://arrow.apache.org/docs/python/index.html) Parquet Files |

## Custom Serializers

You can create your own serializer by subclassing `artigraph.serializer.Serializer`:

```python
from typing import TypeVar

from artigraph.serializer import Serializer

T = TypeVar("T")


class CustomSerializer(Serializer[T]):

    types = (object,)
    """The types that this serializer can handle"""

    def __init__(self):
        # This must be GLOBALLY unique and stable across versions!
        self.name = "custom-serializer"

    def serialize(self, value: T) -> bytes:
        """serialize the value to bytes"""

    def deserialize(self, value: bytes) -> T:
        """deserialize the value from bytes"""


CustomSerializer().register()
```
