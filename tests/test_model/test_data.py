from __future__ import annotations

from typing import Annotated, Any

from artigraph.api.node import create_current
from artigraph.model.base import read_child_models, read_model, write_child_models, write_model
from artigraph.model.data import DataModel
from artigraph.orm import Node
from artigraph.serializer.json import json_serializer
from artigraph.storage.file import temp_file_storage


class SampleModel(DataModel, version=1):
    """A simple artifact model that stores a few basic artifact."""

    some_value: str
    remote_value: Annotated[Any, json_serializer, temp_file_storage]
    inner_model: None | SampleModel = None


async def test_save_load_simple_artifact_model():
    """Test saving and loading a simple artifact model."""
    artifact = SampleModel(some_value="test-value", remote_value={"some": "data"})
    artifact_id = await write_model("some-label", artifact)
    loaded_artifact = await read_model(artifact_id)
    assert loaded_artifact == artifact


async def test_read_write_simple_artifact_model_with_inner_model():
    """Test saving and loading a simple artifact model with an inner model."""
    inner_inner_artifact = SampleModel(
        some_value="inner-inner-value",
        remote_value={"inner-inner": "data"},
    )
    inner_artifact = SampleModel(
        some_value="inner-value",
        remote_value={"inner": "data"},
        inner_model=inner_inner_artifact,
    )
    artifact = SampleModel(
        some_value="test-value",
        remote_value={"some": "data"},
        inner_model=inner_artifact,
    )

    artifact_id = await write_model("some-label", artifact)
    loaded_artifact = await read_model(artifact_id)
    assert loaded_artifact == artifact


async def test_read_write_child_artifact_models():
    """Test saving and loading a simple artifact model with child models."""
    async with create_current(Node):
        models = {"label": SampleModel(some_value="test-value", remote_value={"some": "data"})}
        await write_child_models("current", models=models)
        assert await read_child_models("current") == models
