# Serializers

Artifacts are serialized before being stored in the database. This allows you to store
arbitrary data types in your artifacts. You can use the
[built-in serializers](#built-in-serializers) or you can
[create your own](#custom-serializers).

## Built-in Serializers

| Serializer                                                                            | Description                                                              |
| ------------------------------------------------------------------------------------- | ------------------------------------------------------------------------ |
| [core.serializer.datetime.datetime_serializer][artigraph.datetime_serializer]         | Date Times                                                               |
| [core.serializer.json.json_serializer][artigraph.json_serializer]                     | JSON                                                                     |
| [core.serializer.json.json_sorted_serializer][artigraph.json_sorted_serializer]       | JSON with sorted keys                                                    |
| [extra.numpy.array_serializer][artigraph.extras.numpy.array_serializer]               | 1d and 2d [Numpy](https://numpy.org/) Arrays                             |
| [extra.pandas.dataframe_serializer][artigraph.extras.pandas.dataframe_serializer]     | [Pandas](https://pandas.pydata.org/) DataFrames                          |
| [extra.plotly.figure_json_serializer][artigraph.extras.plotly.figure_json_serializer] | [Plotly](https://plotly.com/python/) Figures                             |
| [extra.polars.dataframe_serializer][artigraph.extras.polars.dataframe_serializer]     | [Polars](https://pola-rs.github.io/) DataFrames                          |
| [extra.pyarrow.feather_serializer][artigraph.extras.pyarrow.feather_serializer]       | [PyArrow](https://arrow.apache.org/docs/python/index.html) Feather Files |
| [extra.pyarrow.parquet_serializer][artigraph.extras.pyarrow.parquet_serializer]       | [PyArrow](https://arrow.apache.org/docs/python/index.html) Parquet Files |

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
