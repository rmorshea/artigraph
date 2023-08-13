import pytest

from artigraph.api.artifact import new_artifact, write_artifact
from artigraph.api.filter import NodeRelationshipFilter, ValueFilter
from artigraph.api.node import new_node, write_node
from artigraph.db import new_session
from artigraph.model.base import (
    MODEL_TYPE_BY_NAME,
    MODELED_TYPES,
    BaseModel,
    _try_convert_value_to_modeled_type,
    allow_model_type_overwrites,
    read_model,
    read_model_or_none,
    read_models,
    write_model,
    write_models,
)
from artigraph.model.data import DataModel
from artigraph.model.filter import ModelFilter, ModelTypeFilter
from artigraph.orm.node import Node
from artigraph.serializer.json import json_serializer


class XModel(DataModel, version=1):
    x: int


class XYModel(XModel, version=1):
    y: int


class XYZModel(XYModel, version=1):
    z: int


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
    kwargs = {k: v for k, (v, _) in _try_convert_value_to_modeled_type(value).model_data().items()}
    model_type = MODELED_TYPES[type(value)]
    assert value == model_type.model_init(model_type.model_version, kwargs)


async def test_read_model_error_if_not_model_node():
    """Test that an error is raised if the node is not a model node."""
    async with new_session(expire_on_commit=False):
        node = await write_node(Node(node_parent_id=None))

        with pytest.raises(ValueError):
            await read_model(ModelFilter(node_id=ValueFilter(eq=node.node_id)))

        qual = await write_artifact(new_artifact("test", "test", json_serializer))

        with pytest.raises(ValueError):
            await read_model(ModelFilter(node_id=ValueFilter(eq=qual.artifact.node_id)))


def test_cannot_define_model_with_same_name():
    class SomeRandomTestModel(BaseModel, version=1):
        pass

    with pytest.raises(RuntimeError):

        class AnotherClass(BaseModel, version=1):
            model_name = SomeRandomTestModel.model_name
            pass


async def test_filter_on_model_type():
    root = await write_node(new_node())
    x_model = XModel(x=1)
    xy_model = XYModel(x=1, y=2)
    await write_models(parent_id=root.node_id, models={"x": x_model, "xy": xy_model})
    db_model = await read_model(
        ModelFilter(
            relationship=NodeRelationshipFilter(child_of=root.node_id),
            model_type=ModelTypeFilter(type=XModel, subclasses=False),
        )
    )
    assert db_model.value == x_model


async def test_filter_on_model_type_with_subclasses():
    root = await write_node(new_node())
    x_model = XModel(x=1)
    xy_model = XYModel(x=1, y=2)
    xyz_model = XYZModel(x=1, y=2, z=3)
    await write_models(
        parent_id=root.node_id, models={"x": x_model, "xy": xy_model, "xyz": xyz_model}
    )
    db_models = await read_models(
        ModelFilter(
            relationship=NodeRelationshipFilter(child_of=root.node_id),
            model_type=ModelTypeFilter(type=XModel, subclasses=True),
        )
    )
    db_models_dict = {dbm.artifact.artifact_label: dbm.value for dbm in db_models}
    assert db_models_dict == {"x": x_model, "xy": xy_model, "xyz": xyz_model}


async def test_read_model_or_none():
    """Test saving and loading a simple artifact model with child models."""
    model = XModel(x=1)
    qual = await write_model(parent_id=None, label="some-label", model=model)
    db_model = await read_model_or_none(ModelFilter(node_id=qual.artifact.node_id))
    assert db_model.value == model
    assert await read_model_or_none(ModelFilter(node_id=1234)) is None


async def test_model_migration():
    class SomeModel(DataModel, version=1):
        old_field_name: int

    old_model = SomeModel(old_field_name=1)
    node_id = (await write_model(label="test", model=old_model)).artifact.node_id

    del MODEL_TYPE_BY_NAME[SomeModel.__name__]

    class SomeModel(DataModel, version=2):
        new_field_name: int

        @classmethod
        def model_init(cls, version, kwargs):
            if version == 1:
                kwargs["new_field_name"] = kwargs.pop("old_field_name")
            return super().model_init(2, kwargs)

    new_model = await read_model(ModelFilter(node_id=node_id))
    assert not hasattr(new_model.value, "old_field_name")
    assert new_model.value.new_field_name == 1


async def test_filter_by_model_version():
    with allow_model_type_overwrites():

        class SomeModel(DataModel, version=1):
            pass

        old_model = SomeModel()
        await write_model(label="test", model=old_model)
        assert (await read_model(ModelFilter())).artifact.model_artifact_version == 1

        class SomeModel(DataModel, version=2):
            pass

        new_model = SomeModel()
        await write_model(label="test", model=new_model)

        assert (
            await read_model(ModelFilter(model_type=ModelTypeFilter(type=SomeModel, version=2)))
        ).artifact.model_artifact_version == 2
