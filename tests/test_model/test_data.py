from __future__ import annotations

from typing import Annotated, Any

from artigraph.model.base import read_model, write_model
from artigraph.model.data import DataModel
from artigraph.serializer.json import json_serializer
from artigraph.storage.file import temp_file_storage


class SimpleModel(DataModel, version=1):
    """A simple artifact model that stores a few basic artifact."""

    some_value: str
    remote_value: Annotated[Any, json_serializer, temp_file_storage]
    inner_model: None | SimpleModel = None


async def test_save_load_simple_artifact_model():
    """Test saving and loading a simple artifact model."""
    artifact = SimpleModel(some_value="test-value", remote_value={"some": "data"})
    artifact_id = await write_model("some-label", artifact)
    loaded_artifact = await read_model(artifact_id)
    assert loaded_artifact == artifact


async def test_save_load_simple_artifact_model_with_inner_model():
    """Test saving and loading a simple artifact model with an inner model."""
    inner_inner_artifact = SimpleModel(
        some_value="inner-inner-value",
        remote_value={"inner-inner": "data"},
    )
    inner_artifact = SimpleModel(
        some_value="inner-value",
        remote_value={"inner": "data"},
        inner_model=inner_inner_artifact,
    )
    artifact = SimpleModel(
        some_value="test-value",
        remote_value={"some": "data"},
        inner_model=inner_artifact,
    )

    artifact_id = await write_model("some-label", artifact)
    loaded_artifact = await read_model(artifact_id)
    assert loaded_artifact == artifact
