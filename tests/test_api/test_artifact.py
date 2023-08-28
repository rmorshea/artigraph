from artigraph.api.artifact import Artifact
from artigraph.api.filter import ArtifactFilter
from artigraph.api.func import delete_one, exists, read_one, write_one
from artigraph.serializer.json import json_serializer
from artigraph.storage.file import temp_file_storage


async def test_write_read_delete_database_artifact():
    artifact = Artifact(value={"test": "data"}, serializer=json_serializer)
    artifact_filter = ArtifactFilter(node_id=artifact.node_id)

    await write_one(artifact)

    db_artifact = await read_one(Artifact, artifact_filter)
    assert db_artifact.value == {"test": "data"}

    await delete_one(artifact)
    assert not await exists(Artifact, artifact_filter)


async def test_write_read_delete_remote_artifact():
    artifact = Artifact(
        value={"test": "data"},
        serializer=json_serializer,
        storage=temp_file_storage,
    )
    artifact_filter = ArtifactFilter(node_id=artifact.node_id)

    await write_one(artifact)

    db_artifact = await read_one(Artifact, artifact_filter)
    assert db_artifact.value == {"test": "data"}

    await delete_one(artifact)
    assert not await exists(Artifact, artifact_filter)
