from artigraph.api.artifact import create_artifact, delete_artifacts, read_artifact_by_id
from artigraph.api.node import node_exists
from artigraph.orm.artifact import DatabaseArtifact, RemoteArtifact
from artigraph.serializer.json import json_serializer
from artigraph.storage.file import temp_file_storage


async def test_create_read_delete_database_artifact():
    """Test creating an artifact."""
    artifact = DatabaseArtifact(
        node_parent_id=None,
        artifact_label="test-label",
        artifact_serializer=json_serializer.name,
    )
    artifact_id = await create_artifact(artifact, {"some": "data"})

    artifact, db_artifact_value = await read_artifact_by_id(artifact_id)
    assert isinstance(artifact, DatabaseArtifact)
    assert db_artifact_value == {"some": "data"}

    await delete_artifacts([artifact])
    assert not (await node_exists(artifact_id))


async def test_create_read_delete_remote_artifact():
    """Test creating an artifact."""
    artifact = RemoteArtifact(
        node_parent_id=None,
        artifact_label="test-label",
        remote_artifact_storage=temp_file_storage.name,
        artifact_serializer=json_serializer.name,
    )
    artifact_id = await create_artifact(artifact, {"some": "data"})

    artifact, db_artifact_value = await read_artifact_by_id(artifact_id)
    assert isinstance(artifact, RemoteArtifact)
    assert db_artifact_value == {"some": "data"}

    await delete_artifacts([artifact])

    assert not await temp_file_storage.exists(artifact.remote_artifact_location)
