# Models

A [GraphModel][argigraph.GraphModel] provides a way to store structure data in
Artigraph. You can use one of the [built-in models](#built-in-models). Alternatively,
you can create your own by subclassing it. You need only implement two methods:

-   [graph_model_data][artigraph.GraphModel.graph_model_data]
-   [graph_model_init][artigraph.GraphModel.graph_model_init]

```python
TODO
```

## Built-in Models

| Model                                                                              | Description                                                                                                      |
| ---------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------- |
| [core.model.dataclasses.dataclass][artigraph.dataclass]                            | A model that uses the standard library [dataclasses](https://docs.python.org/3/library/dataclasses.html) module. |
| [extra.model.pydantic.PydanticModel][artigraph.extra.model.pydantic.PydanticModel] | A model that uses the [Pydantic](https://docs.pydantic.dev/) library.                                            |

## Pydantic

TODO
