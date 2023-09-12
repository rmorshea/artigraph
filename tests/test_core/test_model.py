from __future__ import annotations

import pytest

from artigraph.core.api.filter import ModelFilter, NodeFilter, NodeLinkFilter
from artigraph.core.api.funcs import delete_one, exists, read_one, write_one
from artigraph.core.api.link import NodeLink
from artigraph.core.api.node import Node
from artigraph.core.model.base import MODELED_TYPES, _try_convert_value_to_modeled_type
from artigraph.core.model.dataclasses import dataclass


@dataclass(version=1)
class SimpleModel:
    x: int
    y: str
    z: SimpleModel | None = None


async def test_write_read_delete_non_nested_dataclass_model():
    model = SimpleModel(x=1, y="2")
    model_filter = ModelFilter(node_id=model.graph_node_id, model_type=SimpleModel)

    await write_one(model)
    assert await exists(SimpleModel, model_filter)
    assert await exists(Node, NodeFilter(descendant_of=model.graph_node_id))
    assert await exists(NodeLink, NodeLinkFilter(ancestor=model.graph_node_id))

    db_model = await read_one(SimpleModel, model_filter)
    assert db_model == model

    await delete_one(model)
    assert not await exists(SimpleModel, model_filter)
    assert not await exists(Node, NodeFilter(descendant_of=model.graph_node_id))
    assert not await exists(NodeLink, NodeLinkFilter(ancestor=model.graph_node_id))


async def test_write_read_delete_nested_dataclass_model():
    model = SimpleModel(x=1, y="2", z=SimpleModel(x=3, y="4", z=SimpleModel(x=5, y="6")))
    model_filter = ModelFilter(node_id=model.graph_node_id, model_type=SimpleModel)

    await write_one(model)
    assert await exists(SimpleModel, model_filter)
    assert await exists(Node, NodeFilter(descendant_of=model.graph_node_id))
    assert await exists(NodeLink, NodeLinkFilter(ancestor=model.graph_node_id))

    db_model = await read_one(SimpleModel, model_filter)
    assert db_model == model

    await delete_one(model)
    assert not await exists(SimpleModel, model_filter)
    assert not await exists(Node, NodeFilter(descendant_of=model.graph_node_id))
    assert not await exists(NodeLink, NodeLinkFilter(ancestor=model.graph_node_id))


@pytest.mark.parametrize(
    "value",
    [
        {"some": "data"},
        ("some", "data"),
        ["some", "data"],
        frozenset({"some", "data"}),
        {"some", "data"},
    ],
)
def test_try_convert_value_to_and_from_modeled_type(value):
    kwargs = {
        k: v for k, (v, _) in _try_convert_value_to_modeled_type(value).graph_model_data().items()
    }
    model_type = MODELED_TYPES[type(value)]
    assert value == model_type.graph_model_init(model_type.graph_model_version, kwargs)
