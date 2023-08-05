from artigraph.api.artifact import (
    create_artifact,
    create_artifacts,
    delete_artifacts,
    new_artifact,
    read_artifact_by_id,
    read_child_artifacts,
)
from artigraph.api.node import create_node, node_exists
from artigraph.orm.artifact import DatabaseArtifact, RemoteArtifact
from artigraph.orm.node import Node
from artigraph.serializer.json import json_serializer
from artigraph.storage.file import temp_file_storage


async def test_create_read_delete_database_artifact():
    """Test creating an artifact."""
    artifact = DatabaseArtifact(
        node_parent_id=None,
        artifact_label="test-label",
        artifact_serializer=json_serializer.name,
        artifact_detail="something",
    )
    artifact_id = await create_artifact(artifact, {"some": "data"})

    artifact, db_artifact_value = await read_artifact_by_id(artifact_id)
    assert isinstance(artifact, DatabaseArtifact)
    assert db_artifact_value == {"some": "data"}

    await delete_artifacts([artifact])
    assert not (await node_exists(artifact_id))


async def test_create_read_delete_remote_artifact():
    """Test creating an artifact."""
    artifact, value = new_artifact(
        label="test-label",
        value={"some": "data"},
        detail="something",
        storage=temp_file_storage,
        serializer=json_serializer,
    )
    artifact_id = await create_artifact(artifact, value)

    artifact, db_artifact_value = await read_artifact_by_id(artifact_id)
    assert isinstance(artifact, RemoteArtifact)
    assert db_artifact_value == {"some": "data"}

    await delete_artifacts([artifact])

    assert not await temp_file_storage.exists(artifact.remote_artifact_location)


async def test_read_child_artifacts():
    node = await create_node(Node(node_parent_id=None), refresh_attributes=["node_id"])
    qual_artifacts = [new_artifact(str(i), i, parent_id=node.node_id) for i in range(10)]
    artifact_ids = set(await create_artifacts(qual_artifacts))
    db_artifact_ids = {a.node_id for a, _ in await read_child_artifacts(node.node_id)}
    assert db_artifact_ids == artifact_ids
