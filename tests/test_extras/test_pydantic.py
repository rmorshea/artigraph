from __future__ import annotations

from artigraph.core.api.filter import LinkFilter, NodeFilter
from artigraph.core.api.funcs import delete_one, exists, read_one, write_one
from artigraph.core.api.link import Link
from artigraph.core.api.node import Node
from artigraph.core.model.filter import ModelFilter
from artigraph.extras.pydantic import PydanticModel


class PydanticSimpleModel(PydanticModel, version=1):
    x: int
    y: str
    z: PydanticSimpleModel | None = None


async def test_write_read_delete_non_nested_dataclass_model():
    model = PydanticSimpleModel(x=1, y="2")
    model_filter = ModelFilter(id=model.graph_id, model_type=PydanticSimpleModel)

    await write_one.a(model)
    assert await exists.a(PydanticSimpleModel, model_filter)
    assert await exists.a(Node, NodeFilter(descendant_of=model.graph_id))
    assert await exists.a(Link, LinkFilter(ancestor=model.graph_id))

    db_model = await read_one.a(PydanticSimpleModel, model_filter)
    assert db_model == model

    await delete_one.a(model)
    assert not await exists.a(PydanticSimpleModel, model_filter)
    assert not await exists.a(Node, NodeFilter(descendant_of=model.graph_id))
    assert not await exists.a(Link, LinkFilter(ancestor=model.graph_id))


async def test_write_read_delete_nested_dataclass_model():
    model = PydanticSimpleModel(
        x=1,
        y="2",
        z=PydanticSimpleModel(x=3, y="4", z=PydanticSimpleModel(x=5, y="6")),
    )
    model_filter = ModelFilter(id=model.graph_id, model_type=PydanticSimpleModel)

    await write_one.a(model)
    assert await exists.a(PydanticSimpleModel, model_filter)
    assert await exists.a(Node, NodeFilter(descendant_of=model.graph_id))
    assert await exists.a(Link, LinkFilter(ancestor=model.graph_id))

    db_model = await read_one.a(PydanticSimpleModel, model_filter)
    assert db_model == model

    await delete_one.a(model)
    assert not await exists.a(PydanticSimpleModel, model_filter)
    assert not await exists.a(Node, NodeFilter(descendant_of=model.graph_id))
    assert not await exists.a(Link, LinkFilter(ancestor=model.graph_id))
