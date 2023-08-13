from __future__ import annotations

import pytest

from artigraph.api.node import new_node, write_node
from artigraph.model.data import DataModel
from artigraph.model.group import ModelGroup, current_model_group


class SimpleModel(DataModel, version=1):
    """A simple model."""

    x: int
    y: int


async def test_model_simple_group():
    """Test that a model group can be created and saved."""
    node = new_node()
    async with ModelGroup(node) as group:
        model = SimpleModel(x=1, y=2)
        group.add_model("test", model)

    assert await group.has_model("test", fresh=True)
    db_model = await group.get_model("test", fresh=True)
    assert db_model is not model
    assert db_model == model


async def test_get_current_model():
    """Test that the current model group can be retrieved."""
    node = new_node()

    def add_simple_model():
        current_model_group().add_model("test", SimpleModel(x=1, y=2))

    async with ModelGroup(node) as group:
        add_simple_model()

    db_model = await group.get_model("test", fresh=True)
    assert db_model == SimpleModel(x=1, y=2)


async def test_cannot_add_model_with_same_label():
    """Test that a model cannot be added with the same label as an existing model."""
    node = new_node()
    async with ModelGroup(node) as group:
        group.add_model("test", SimpleModel(x=1, y=2))
        with pytest.raises(ValueError, match=r"Models with labels .* already exist in this group"):
            group.add_model("test", SimpleModel(x=1, y=2))


async def test_get_many_models_from_group():
    """Test that many models can be retrieved from a group."""
    node = new_node()
    async with ModelGroup(node) as group:
        group.add_model("test1", SimpleModel(x=1, y=2))
        group.add_model("test2", SimpleModel(x=3, y=4))
        group.add_model("test3", SimpleModel(x=5, y=6))

    models = await group.get_models(["test1", "test2", "test3"], fresh=True)
    assert models == {
        "test1": SimpleModel(x=1, y=2),
        "test2": SimpleModel(x=3, y=4),
        "test3": SimpleModel(x=5, y=6),
    }


async def test_get_all_models_from_group():
    """Test that many models can be retrieved from a group."""
    node = new_node()
    async with ModelGroup(node) as group:
        group.add_model("test1", SimpleModel(x=1, y=2))
        group.add_model("test2", SimpleModel(x=3, y=4))
        group.add_model("test3", SimpleModel(x=5, y=6))

    models = await group.get_models()
    assert models == {
        "test1": SimpleModel(x=1, y=2),
        "test2": SimpleModel(x=3, y=4),
        "test3": SimpleModel(x=5, y=6),
    }


async def test_get_models_no_refresh():
    node = new_node()

    group = ModelGroup(node)
    model1 = SimpleModel(x=1, y=2)
    group.add_model("test1", model1)
    await group.save()

    # recreate the group so it's not in the cache
    group = ModelGroup(node)
    model2 = SimpleModel(x=3, y=4)
    group.add_model("test2", model2)
    await group.save()

    # get the models without refreshing
    models = await group.get_models(["test1", "test2"], fresh=False)

    got_model1 = models["test1"]
    assert got_model1 is not model1
    assert got_model1 == model1

    got_model2 = models["test2"]
    assert got_model2 is model2
    assert got_model2 == model2


async def test_remove_models():
    node = new_node()
    async with ModelGroup(node) as group:
        group.add_model("test1", SimpleModel(x=1, y=2))
        group.add_model("test2", SimpleModel(x=3, y=4))

    assert await group.has_model("test1")
    assert await group.has_model("test2")

    await group.remove_model("test1")

    assert not await group.has_model("test1")
    assert await group.has_model("test2")


async def test_remove_all_models():
    node = new_node()
    async with ModelGroup(node) as group:
        group.add_model("test1", SimpleModel(x=1, y=2))
        group.add_model("test2", SimpleModel(x=3, y=4))

    assert await group.has_model("test1")
    assert await group.has_model("test2")

    await group.remove_models()

    assert not await group.has_model("test1")
    assert not await group.has_model("test2")


async def test_get_parent_group():
    async with ModelGroup(new_node()) as outer:
        outer.add_model("test_outer", SimpleModel(x=1, y=2))
        async with ModelGroup(new_node()) as inner:
            inner.add_model("test_inner", SimpleModel(x=3, y=3))

    # sanity check
    assert await outer.get_model("test_outer", fresh=True) == SimpleModel(x=1, y=2)

    got_outer = await inner.get_parent_group()
    assert got_outer is not None
    assert await got_outer.get_model("test_outer", fresh=True) == SimpleModel(x=1, y=2)


async def test_group_from_existing_node_id():
    node = new_node()
    await write_node(node)
    node_id = node.node_id

    group = ModelGroup(node_id)
    model = SimpleModel(x=1, y=2)
    group.add_model("test", model)
    await group.save()

    assert await group.has_model("test")
    db_model = await group.get_model("test", fresh=True)
    assert db_model is not model
    assert db_model == model
    await group.remove_model("test")
    assert not await group.has_model("test")


async def test_get_models_all_cached():
    node = new_node()

    model1 = SimpleModel(x=1, y=2)
    model2 = SimpleModel(x=3, y=4)

    async with ModelGroup(node) as group:
        group.add_model("test1", model1)
        group.add_model("test2", model2)

    models = await group.get_models(["test1", "test2"])
    assert models["test1"] is model1
    assert models["test2"] is model2
