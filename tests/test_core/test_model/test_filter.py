from artigraph.core.api.funcs import read, read_one, write_many
from artigraph.core.api.node import Node
from artigraph.core.model.base import GraphModel
from artigraph.core.model.dataclasses import dataclass
from artigraph.core.model.filter import ModelFilter, ModelTypeFilter


@dataclass
class FirstModelType(GraphModel, version=1):
    x: int


@dataclass
class FirstModelTypeSub(FirstModelType, version=1):
    y: int


@dataclass
class SecondModelType(GraphModel, version=1):
    z: int


async def test_filter_by_model_type():
    first = FirstModelType(x=1)
    second = SecondModelType(z=2)

    await write_many.a([first, second])
    model_filter = ModelFilter(model_type=FirstModelType)
    assert type(await read_one.a(GraphModel, model_filter)) == FirstModelType


async def test_filter_by_module_type_with_subclasses():
    first = FirstModelType(x=1)
    first_sub = FirstModelTypeSub(x=1, y=2)

    await write_many.a([first, first_sub])
    model_filter = ModelFilter(model_type=ModelTypeFilter(type=FirstModelType, subclasses=True))
    db_model_types = [type(m) for m in await read.a(GraphModel, model_filter)]
    assert sorted(db_model_types, key=lambda t: t.__name__) == [
        FirstModelType,
        FirstModelTypeSub,
    ]


async def test_filter_by_module_type_no_subclasses():
    first = FirstModelType(x=1)
    first_sub = FirstModelTypeSub(x=1, y=2)

    await write_many.a([first, first_sub])
    model_filter = ModelFilter(model_type=ModelTypeFilter(type=FirstModelType, subclasses=False))
    assert type(await read_one.a(GraphModel, model_filter)) == FirstModelType


async def test_default_model_filter():
    node = Node()
    model = FirstModelType(x=1)
    await write_many.a([node, model])

    assert type(await read_one.a(GraphModel, ModelFilter())) == FirstModelType
