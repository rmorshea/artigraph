import pytest

from artigraph.api.artifact import new_artifact, write_artifact
from artigraph.api.filter import ValueFilter
from artigraph.api.node import write_node
from artigraph.db import new_session
from artigraph.model.base import (
    MODELED_TYPES,
    BaseModel,
    _get_model_type_by_name,
    _try_convert_value_to_modeled_type,
    read_model,
)
from artigraph.model.filter import ModelFilter
from artigraph.orm.node import Node
from artigraph.serializer.json import json_serializer


def _get_model_date(model):
    data = {}
    for k, (v, _) in model.model_data().items():
        if isinstance(v, BaseModel):
            data[k] = _get_model_date(v)
        else:
            data[k] = v
    return data


def test_get_model_type_by_name_unknown_name():
    """Test the get_model_type_by_name function."""
    with pytest.raises(ValueError):
        assert _get_model_type_by_name("unknown") is None


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
