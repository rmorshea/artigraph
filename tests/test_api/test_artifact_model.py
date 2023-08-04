from dataclasses import dataclass, field
from typing import Any

from artigraph.api.artifact_model import (
    ArtifactMapping,
    ArtifactModel,
    ArtifactSequence,
    artifact_field,
)
from artigraph.serializer.json import json_serializer
from artigraph.storage.file import temp_file_storage


@dataclass(frozen=True)
class SimpleArtifactModel(ArtifactModel, version=1):
    """A simple artifact model that stores a few basic artifact."""

    some_value: str
    remote_value: Any = artifact_field(serializer=json_serializer, storage=temp_file_storage)
    inner_model: "None | SimpleArtifactModel" = None


async def test_save_load_simple_artifact_model():
    """Test saving and loading a simple artifact model."""
    artifact = SimpleArtifactModel(some_value="test-value", remote_value={"some": "data"})
    artifact_id = await artifact.create(None)
    loaded_artifact = await SimpleArtifactModel.read(artifact_id)
    assert loaded_artifact == artifact


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

    artifact_id = await artifact.create(None)
    loaded_artifact = await SimpleArtifactModel.read(artifact_id)
    assert loaded_artifact == artifact


@dataclass(frozen=True)
class ComplexArtifactModel(ArtifactModel, version=1):
    """A complex artifact model that stores a few basic artifact."""

    simple: SimpleArtifactModel
    mapping: ArtifactMapping["ComplexArtifactModel"] = field(default_factory=dict)
    sequence: ArtifactSequence["ComplexArtifactModel"] = field(default_factory=dict)


async def test_save_load_complex_artifact_model():
    """Test saving and loading a complex artifact model."""
    simple = SimpleArtifactModel(some_value="test-value", remote_value={"some": "data"})
    artifact = ComplexArtifactModel(
        simple=simple,
        mapping=ArtifactMapping(
            key1=ComplexArtifactModel(
                simple=simple,
                mapping=ArtifactMapping(key1=ComplexArtifactModel(simple=simple)),
            ),
            key2=ComplexArtifactModel(simple=simple),
        ),
        sequence=ArtifactSequence(
            [
                ComplexArtifactModel(simple=simple),
                ComplexArtifactModel(
                    simple=simple,
                    mapping=ArtifactMapping(key3=ComplexArtifactModel(simple=simple)),
                ),
            ]
        ),
    )

    artifact_id = await artifact.create(None)
    loaded_artifact = await ComplexArtifactModel.read(artifact_id)
    assert loaded_artifact == artifact
