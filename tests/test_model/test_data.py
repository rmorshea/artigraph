from __future__ import annotations

from dataclasses import fields
from typing import Annotated, Any, Optional

import pandas as pd

from artigraph.api.filter import NodeRelationshipFilter
from artigraph.api.node import new_node, write_node
from artigraph.model.base import (
    read_model,
    write_model,
    write_models,
)
from artigraph.model.data import DataModel
from artigraph.model.filter import ModelFilter, ModelTypeFilter
from artigraph.serializer.pandas import dataframe_serializer
from artigraph.storage.file import temp_file_storage

FileDataFrame = Annotated[pd.DataFrame, dataframe_serializer, temp_file_storage]


class SampleModel(DataModel, version=1):
    """A simple artifact model that stores a few basic artifact."""

    some_value: str
    remote_value: FileDataFrame
    inner_model: Optional[SampleModel] = None

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, SampleModel):
            return False
        for f in fields(self):
            value = getattr(self, f.name)
            if isinstance(value, pd.DataFrame):
                if not value.equals(getattr(other, f.name)):
                    return False
            elif value != getattr(other, f.name):
                return False
        return True


async def test_save_load_simple_artifact_model():
    """Test saving and loading a simple artifact model."""
    model = SampleModel(some_value="test-value", remote_value=pd.DataFrame())
    qual = await write_model(parent_id=None, label="some-label", model=model)
    db_model = await read_model(ModelFilter(node_id=qual.artifact.node_id))
    assert db_model.value == model


async def test_read_write_simple_artifact_model_with_inner_model():
    """Test saving and loading a simple artifact model with an inner model."""
    inner_inner_model = SampleModel(
        some_value="inner-inner-value",
        remote_value=pd.DataFrame(),
    )
    inner_model = SampleModel(
        some_value="inner-value",
        remote_value=pd.DataFrame(),
        inner_model=inner_inner_model,
    )
    model = SampleModel(
        some_value="test-value",
        remote_value=pd.DataFrame(),
        inner_model=inner_model,
    )

    qual = await write_model(parent_id=None, label="some-label", model=model)
    db_model = await read_model(ModelFilter(node_id=qual.artifact.node_id))
    assert db_model.value == model


class OtherModel(SampleModel, version=1):
    pass


async def test_filter_on_model_type():
    """Test filtering on model type exclude OtherModel"""
    root = await write_node(new_node())
    sample_model = SampleModel(some_value="sample-value", remote_value=pd.DataFrame())
    other_model = OtherModel(some_value="other-value", remote_value=pd.DataFrame())
    await write_models(
        parent_id=root.node_id, models={"sample": sample_model, "other": other_model}
    )
    db_model = await read_model(
        ModelFilter(
            relationship=NodeRelationshipFilter(child_of=root.node_id),
            model_type=ModelTypeFilter(type=SampleModel, subclasses=False),
        )
    )
    assert db_model.value == sample_model


# async def test_read_write_child_artifact_models():
#     """Test saving and loading a simple artifact model with child models."""
#     async with create_current(Node):
#         models = {
#             "label": SampleModel(some_value="test-value", remote_value=pd.DataFrame()),
#             "other_label": SampleModel(some_value="other-value", remote_value=pd.DataFrame()),
#             "another_label": SampleModel(some_value="another-value", remote_value=pd.DataFrame()),
#         }
#         await write_models("current", models=models)
#         assert await read_child_models("current", labels=["label", "other_label"]) == {
#             "label": models["label"],
#             "other_label": models["other_label"],
#         }
#         assert await read_child_models("current", labels=["does_not_exist"]) == {}
