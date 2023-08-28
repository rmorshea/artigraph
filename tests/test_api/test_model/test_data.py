from __future__ import annotations

from dataclasses import fields
from typing import Annotated, Any, Optional

import pandas as pd

from artigraph.model.base import (
    read_model,
    write_model,
)
from artigraph.model.data import DataModel
from artigraph.model.filter import ModelFilter
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


async def test_read_write_simple_data_model_with_inner_model():
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
