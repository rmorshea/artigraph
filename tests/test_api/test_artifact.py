from artigraph.api.artifact import (
    delete_artifact,
    new_artifact,
    read_artifact_by_id,
    read_child_artifacts,
    write_artifact,
    write_artifacts,
)
from artigraph.api.node import (
    read_node_exists,
    read_nodes_exist,
    write_node,
    write_parent_child_relationships,
)
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

    await delete_artifact(artifact_id)
    assert not await read_node_exists(artifact_id)


async def test_create_read_delete_remote_artifact():
    """Test creating an artifact."""
    artifact, value = new_artifact(
        label="test-label",
        value={"some": "data"},
        serializer=json_serializer,
        detail="something",
        storage=temp_file_storage,
    )
    artifact_id = await write_artifact(artifact, value)

    artifact, db_artifact_value = await read_artifact_by_id(artifact_id)
    assert isinstance(artifact, RemoteArtifact)
    assert db_artifact_value == {"some": "data"}

    await delete_artifact(artifact_id)
    assert not await read_node_exists(artifact_id)
    assert not await temp_file_storage.exists(artifact.remote_artifact_location)


async def test_delete_artifact_descendants():
    grandparent = new_artifact("gp", 1, json_serializer, storage=temp_file_storage)
    parent = new_artifact("p", 2, json_serializer)
    child = new_artifact("c", 3, json_serializer)
    grandchild = new_artifact("gc", 4, json_serializer, storage=temp_file_storage)

    artifacts = [grandparent, parent, child, grandchild]
    remote_artifacts = [grandparent, grandchild]

    # write artifacts
    artifact_ids = await write_artifacts(artifacts)

    # create parent-child relationships
    await write_parent_child_relationships(
        [
            (None, grandparent[0].node_id),
            (grandparent[0].node_id, parent[0].node_id),
            (parent[0].node_id, child[0].node_id),
            (child[0].node_id, grandchild[0].node_id),
        ]
    )

    # delete artifact
    await delete_artifact(grandparent[0].node_id, descendants=True)

    # check that the artifact and its descendants were deleted
    assert not await read_nodes_exist(artifact_ids)

    # check that remote artifacts were deleted
    storage_locations = [a.remote_artifact_location for a, _ in remote_artifacts]

    assert not any([await temp_file_storage.exists(location) for location in storage_locations])


async def test_read_child_artifacts():
    node = await write_node(Node(node_parent_id=None), refresh_attributes=["node_id"])
    qual_artifacts = [
        new_artifact(str(i), i, json_serializer, parent_id=node.node_id) for i in range(10)
    ]
    artifact_ids = set(await write_artifacts(qual_artifacts))
    db_artifact_ids = {a.node_id for a, _ in await read_child_artifacts(node.node_id)}
    assert db_artifact_ids == artifact_ids
