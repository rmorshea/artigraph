from __future__ import annotations

from artigraph.core.api.node import Node
from artigraph.core.model.group import ModelGroup
from tests.common.model import SimpleDataclassModel


async def test_model_group_add_and_get_models():
    """Test adding a group to a group."""
    node = Node()

    async with ModelGroup(node) as group:
        model1 = SimpleDataclassModel(1)
        model2 = SimpleDataclassModel(2)
        group.add_model("model1", model1)
        group.add_model("model2", model2)

    models = await group.get_models.a()
    assert models == {"model1": model1, "model2": model2}

    fresh_models = await group.get_models.a(fresh=True)
    assert fresh_models == {"model1": model1, "model2": model2}
