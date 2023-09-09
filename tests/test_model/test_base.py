import pytest

from artigraph.api.filter import ValueFilter
from artigraph.api.funcs import orm_read_one, read, read_one, write, write_one
from artigraph.api.model import (
    MODEL_TYPE_BY_NAME,
    MODELED_TYPES,
    GraphModel,
    ModelArtifact,
    _try_convert_value_to_modeled_type,
    allow_model_type_overwrites,
)
from artigraph.api.node import Node
from artigraph.model.data import DataModel
from artigraph.model.filter import ModelFilter, ModelTypeFilter
from tests.common import sorted_nodes


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
    kwargs = {
        k: v for k, (v, _) in _try_convert_value_to_modeled_type(value).graph_model_data().items()
    }
    model_type = MODELED_TYPES[type(value)]
    assert value == model_type.graph_model_init(model_type.graph_model_version, kwargs)


async def test_read_model_error_if_not_model_node():
    """Test that an error is raised if the node is not a model node."""
    node = Node()
    await write_one(node)
    with pytest.raises(ValueError):
        await read_one(ModelArtifact, ModelFilter(node_id=ValueFilter(eq=node.node_id)))


def test_cannot_define_model_with_same_name():
    class SomeRandomTestModel(GraphModel, version=1):
        pass

    with pytest.raises(RuntimeError):

        class AnotherClass(GraphModel, version=1):
            model_name = SomeRandomTestModel.graph_model_name
            pass


async def test_filter_on_model_type():
    x_model = ModelArtifact(value=XModel(x=1))
    xy_model = ModelArtifact(value=XYModel(x=1, y=2))
    await write([x_model, xy_model])
    db_model = await read_one(
        ModelArtifact,
        ModelFilter(model_type=ModelTypeFilter(type=XModel, subclasses=False)),
    )
    assert db_model == x_model


async def test_filter_on_model_type_with_subclasses():
    x_model = ModelArtifact(value=XModel(x=1))
    xy_model = ModelArtifact(value=XYModel(x=1, y=2))
    await write([x_model, xy_model])

    db_model = await read(
        ModelArtifact,
        ModelFilter(model_type=ModelTypeFilter(type=XModel, subclasses=True)),
    )

    assert sorted_nodes(db_model) == sorted_nodes([x_model, xy_model])


async def test_model_migration():
    class SomeModel(DataModel, version=1):  # type: ignore
        old_field_name: int

    old_model = ModelArtifact(value=SomeModel(old_field_name=1))
    await write_one(old_model)

    del MODEL_TYPE_BY_NAME[SomeModel.__name__]

    class SomeModel(DataModel, version=2):  # type: ignore
        new_field_name: int

        @classmethod
        def model_init(cls, version, kwargs):
            if version == 1:
                kwargs["new_field_name"] = kwargs.pop("old_field_name")
            return super().model_init(2, kwargs)

    new_model = await read_one(ModelArtifact, ModelFilter(node_id=old_model.node_id))
    assert not hasattr(new_model.value, "old_field_name")
    assert new_model.value.new_field_name == 1  # type: ignore


async def test_filter_by_model_version():
    with allow_model_type_overwrites():

        class SomeModel(DataModel, version=1):  # type: ignore
            pass

        old_model = ModelArtifact(value=SomeModel())

        class SomeModel(DataModel, version=2):
            pass

        new_model = ModelArtifact(value=SomeModel())

        await write([old_model, new_model])

        db_old_model = await orm_read_one(
            ModelArtifact.graph_orm_type,
            ModelFilter(model_type=ModelTypeFilter(type=SomeModel, version=1)),
        )
        assert db_old_model.model_artifact_version == 1

        db_new_model = await orm_read_one(
            ModelArtifact.graph_orm_type,
            ModelFilter(model_type=ModelTypeFilter(type=SomeModel, version=2)),
        )
        assert db_new_model.model_artifact_version == 2
