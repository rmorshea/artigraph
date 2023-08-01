from dataclasses import dataclass, field
from typing import Any

from artigraph.api.artifact_model import ArtifactModel, RemoteModel
from artigraph.serializer.json import JsonSerializer, json_serializer
from artigraph.storage.file import FileSystemStorage, temp_file_storage


@dataclass(frozen=True)
class TempJsonFile(RemoteModel[Any]):
    """A JSON file that is stored in a temporary file."""

    storage: FileSystemStorage = field(default=temp_file_storage, init=False)
    serializer: JsonSerializer = field(default=json_serializer, init=False)


@dataclass(frozen=True)
class SimpleArtifactModel(ArtifactModel):
    """A simple artifact model that stores a few basic artifact."""

    some_value: str
    remote_value: TempJsonFile
    inner_model: "None | SimpleArtifactModel" = None


async def test_save_load_simple_artifact_model():
    """Test saving and loading a simple artifact model."""
    artifact = SimpleArtifactModel(
        some_value="test-value",
        remote_value=TempJsonFile(value={"some": "data"}),
    )

    artifact_node = await artifact.save(None)

    loaded_artifact = await SimpleArtifactModel.load(artifact_node)
    assert loaded_artifact.some_value == "test-value"
    assert loaded_artifact.remote_value.value == {"some": "data"}
    assert loaded_artifact.inner_model is None
