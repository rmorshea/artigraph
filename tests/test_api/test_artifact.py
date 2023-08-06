from artigraph.api.artifact import (
    new_artifact,
    read_artifact_by_id,
    read_child_artifacts,
    write_artifact,
    write_artifacts,
)
from artigraph.api.node import write_node
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
    artifact_id = await write_artifact(artifact, {"some": "data"})

    artifact, db_artifact_value = await read_artifact_by_id(artifact_id)
    assert isinstance(artifact, DatabaseArtifact)
    assert db_artifact_value == {"some": "data"}

    msg = "TODO: test artifact deletion of descendant artifacts"
    raise AssertionError(msg)


async def test_create_read_delete_remote_artifact():
    """Test creating an artifact."""
    artifact, value = new_artifact(
        label="test-label",
        value={"some": "data"},
        detail="something",
        storage=temp_file_storage,
        serializer=json_serializer,
    )
    artifact_id = await write_artifact(artifact, value)

    artifact, db_artifact_value = await read_artifact_by_id(artifact_id)
    assert isinstance(artifact, RemoteArtifact)
    assert db_artifact_value == {"some": "data"}

    msg = "TODO: test artifact deletion of descendant artifacts"
    raise AssertionError(msg)

    assert not await temp_file_storage.exists(artifact.remote_artifact_location)


async def test_read_child_artifacts():
    node = await write_node(Node(node_parent_id=None), refresh_attributes=["node_id"])
    qual_artifacts = [new_artifact(str(i), i, parent_id=node.node_id) for i in range(10)]
    artifact_ids = set(await write_artifacts(qual_artifacts))
    db_artifact_ids = {a.node_id for a, _ in await read_child_artifacts(node.node_id)}
    assert db_artifact_ids == artifact_ids
