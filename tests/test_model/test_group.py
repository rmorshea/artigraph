from __future__ import annotations

from artigraph.api.node import new_node
from artigraph.model.data import DataModel
from artigraph.model.group import ModelGroup


class SimpleModel(DataModel, version=1):
    """A simple model."""

    x: int
    y: int


async def test_model_simple_group() -> None:
    """Test that a model group can be created and saved."""
    node = new_node()
    async with ModelGroup(node) as group:
        model = SimpleModel(x=1, y=2)
        group.add_model("test", model)
    db_model = await group.read_model("test", refresh=True)
    assert db_model is not model
    assert db_model == model
