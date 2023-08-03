from dataclasses import dataclass
from typing import Any

from artigraph.model.artifact import ArtifactModel, artifact_field
from artigraph.serializer.json import json_serializer
from artigraph.storage.file import temp_file_storage


@dataclass
class SimpleArtifactModel(ArtifactModel, version=1):
    """A simple artifact model that stores a few basic artifact."""

    some_value: str
    remote_value: Any = artifact_field(serializer=json_serializer, storage=temp_file_storage)
    inner_model: "None | SimpleArtifactModel" = None


async def test_save_load_simple_artifact_model():
    """Test saving and loading a simple artifact model."""
    artifact = SimpleArtifactModel(some_value="test-value", remote_value={"some": "data"})

    artifact_node = await artifact.save(None)

    loaded_artifact = await SimpleArtifactModel.load(artifact_node)
    assert loaded_artifact.some_value == "test-value"
    assert loaded_artifact.remote_value.value == {"some": "data"}
    assert loaded_artifact.inner_model is None


async def test_save_load_simple_artifact_model_with_inner_model():
    """Test saving and loading a simple artifact model with an inner model."""
    inner_inner_artifact = SimpleArtifactModel(
        some_value="inner-inner-value",
        remote_value={"inner-inner": "data"},
    )
    inner_artifact = SimpleArtifactModel(
        some_value="inner-value",
        remote_value={"inner": "data"},
        inner_model=inner_inner_artifact,
    )
    artifact = SimpleArtifactModel(
        some_value="test-value",
        remote_value={"some": "data"},
        inner_model=inner_artifact,
    )

    artifact_node = await artifact.save(None)

    loaded_artifact = await SimpleArtifactModel.load(artifact_node)
    assert loaded_artifact.some_value == "test-value"
    assert loaded_artifact.remote_value.value == {"some": "data"}
    assert loaded_artifact.inner_model.some_value == "inner-value"
    assert loaded_artifact.inner_model.remote_value.value == {"inner": "data"}
    assert loaded_artifact.inner_model.inner_model.some_value == "inner-inner-value"
    assert loaded_artifact.inner_model.inner_model.remote_value.value == {"inner-inner": "data"}
