from artigraph.api.artifact import (
    QualifiedArtifact,
    delete_artifacts,
    new_artifact,
    read_artifact,
    write_artifact,
    write_artifacts,
)
from artigraph.api.filter import ArtifactFilter, ValueFilter
from artigraph.api.node import read_nodes_exist, write_node, write_parent_child_relationships
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
    )

    artifact_id = await write_artifact(QualifiedArtifact(artifact, {"some": "data"}))
    artifact_filter = ArtifactFilter(node_id=ValueFilter(eq=artifact_id))

    qual_artifact = await read_artifact(artifact_filter)
    assert isinstance(qual_artifact.artifact, DatabaseArtifact)
    assert qual_artifact.value == {"some": "data"}

    await delete_artifacts(artifact_filter)
    assert not await read_nodes_exist(artifact_filter)


async def test_create_read_delete_remote_artifact():
    """Test creating an artifact."""
    qual_artifact = new_artifact(
        label="test-label",
        value={"some": "data"},
        serializer=json_serializer,
        storage=temp_file_storage,
    )

    artifact_id = await write_artifact(qual_artifact)
    artifact_filter = ArtifactFilter(node_id=ValueFilter(eq=artifact_id))

    db_qual_artifact = await read_artifact(artifact_filter)
    assert isinstance(db_qual_artifact.artifact, RemoteArtifact)
    assert db_qual_artifact.value == {"some": "data"}

    await delete_artifacts(artifact_filter)
    assert not await read_nodes_exist(artifact_filter)
    assert not await temp_file_storage.exists(db_qual_artifact.artifact.remote_artifact_location)


async def test_delete_many_artifacts():
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
