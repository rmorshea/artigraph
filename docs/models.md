# Models

A [GraphModel][artigraph.GraphModel] provides a way to store structured data in
Artigraph.

You can use one of the [built-in models](#built-in-models) or you can
[create your own](#custom-models).

## Built-in Models

| Model                                                                   | Description                                                                                                      |
| ----------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------- |
| [core.model.dataclasses.dataclass][artigraph.dataclass]                 | A model that uses the standard library [dataclasses](https://docs.python.org/3/library/dataclasses.html) module. |
| [extra.pydantic.PydanticModel][artigraph.extras.pydantic.PydanticModel] | A model that uses the [Pydantic](#pydantic/) library.                                                            |

## Pydantic

Artigraph integrates with [Pydantic](https://docs.pydantic.dev/latest/) and provides a
[PydanticModel][artigraph.extras.pydantic.PydanticModel] that you can subclass to get
all the benefits of Pydantic with the added power of being able to store your models as
a graph with Artigraph. with the exception of needing to inherit from `PydanticModel`
and specify a version, you can use it just like you would any other Pydantic model:

```python
import artigraph as ag

from pydantic import Field

class Person(ag.PydanticModel, version=1):
    name: str = Field(..., description="The person's name.")
    age: int = Field(..., description="The person's age.")
```

You can specify serializers and storage for fields just like you would with the built-in
[dataclass model](./building-blocks.md#models):

```python
from typing import Annotated
import artigraph as ag

from pydantic import Field

DateTime = Annotated[datetime, ag.datetime_serializer]

class Person(ag.PydanticModel, version=1):
    name: str = Field(..., description="The person's name.")
    age: int = Field(..., description="The person's age.")
    birthday: DateTime = Field(..., description="The person's birthday.")
```

## Custom Models

To create a custom model, you must subclass `GraphModel`, specify a version, and define
the following:

-   [graph_id][artigraph.GraphModel.graph_id] - Id of the root node of the model.
-   [graph_model_data()][artigraph.GraphModel.graph_model_data] - Data to be stored in
    the database.
-   [graph_model_init()][artigraph.GraphModel.graph_model_init] - Method to initialize
    the model from the data.

Here's an exmaple of a custom model:

```python
from typing import Self
from uuid import UUID, uuid1

import artigraph as ag


class MyModel(ag.GraphModel, version=1):

    def __init__(self, data: Any, graph_id: UUID | None = None) -> None:
        self.data = data
        self.graph_id = graph_id or uuid1()

    def graph_model_data(self) -> dict[str, tuple[Any, ag.SaveSpec]]:
        return {
            "data": (self.data, ag.SaveSpec(serializer=ag.json_serializer)),
        }

    @classmethod
    def graph_model_init(cls, info: ag.ModelInfo, kwargs: dict[str, Any]) -> Self:
        return cls(**kwargs, graph_id=info.graph_id)
```

It's worth noting that the `ModelInfo` supplied to `graph_model_init()` when
initializing a model from the database contains the `version` of the model data from the
database. This allows you to handle migrations if you need to. For example, if you
wanted to rename the `data` field in the model above to `my_data`, you could do so like
this:

```python
class MyModel(ag.GraphModel, version=2):

    def __init__(self, my_data: Any, graph_id: UUID | None = None) -> None:
        self.my_data = my_data
        self.graph_id = graph_id or uuid1()

    def graph_model_data(self) -> dict[str, tuple[Any, ag.SaveSpec]]:
        return {
            "my_data": (self.data, ag.SaveSpec(serializer=ag.json_serializer)),
        }

    @classmethod
    def graph_model_init(cls, info: ag.ModelInfo, kwargs: dict[str, Any]) -> Self:
        if info.version == 1:
            kwargs["my_data"] = kwargs.pop("data")
        return cls(**kwargs, graph_id=info.graph_id)
```
