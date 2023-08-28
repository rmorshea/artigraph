from artigraph.api.artifact import (
    delete_artifacts,
    read_artifact,
    read_artifact_or_none,
    read_artifacts,
    write_artifact,
    write_artifacts,
)
from artigraph.api.filter import ArtifactFilter, NodeLinkFilter, ValueFilter
from artigraph.api.node import (
    exists,
    write_links,
    write_one,
)
from artigraph.orm.artifact import OrmDatabaseArtifact, OrmRemoteArtifact
from artigraph.orm.node import OrmNode
from artigraph.serializer.json import json_serializer
from artigraph.storage.file import temp_file_storage


async def test_create_read_delete_database_artifact():
    """Test creating an artifact."""
    artifact = OrmDatabaseArtifact(
        artifact_label="test-label",
        artifact_serializer=json_serializer.name,
        artifact_data=None,
    )

    qual = await write_artifact(QualifiedArtifact(artifact, {"some": "data"}))
    artifact_filter = ArtifactFilter(node_id=ValueFilter(eq=qual.artifact.node_id))

    qual_artifact = await read_artifact(artifact_filter)
    assert isinstance(qual_artifact.artifact, OrmDatabaseArtifact)
    assert qual_artifact.value == {"some": "data"}

    await delete_artifacts(artifact_filter)
    assert not await exists(artifact_filter)


async def test_read_artifact_or_none():
    """Test reading an artifact that doesn't exist."""
    artifact_filter = ArtifactFilter(node_id=ValueFilter(eq=123))
    assert await read_artifact_or_none(artifact_filter) is None

    artifact = OrmDatabaseArtifact(
        artifact_serializer=json_serializer.name,
        artifact_data=None,
    )
    qual = await write_artifact(QualifiedArtifact(artifact, {"some": "data"}))

    artifact_filter = ArtifactFilter(node_id=ValueFilter(eq=qual.artifact.node_id))
    db_qual = await read_artifact_or_none(artifact_filter)
    assert db_qual is not None
    assert db_qual.value == qual.value


async def test_create_read_delete_remote_artifact():
    """Test creating an artifact."""
    qual_artifact = new_artifact(
        label="test-label",
        value={"some": "data"},
        serializer=json_serializer,
        storage=temp_file_storage,
    )

    qual = await write_artifact(qual_artifact)
    artifact_filter = ArtifactFilter(node_id=ValueFilter(eq=qual.artifact.node_id))

    db_qual_artifact = await read_artifact(artifact_filter)
    assert isinstance(db_qual_artifact.artifact, OrmRemoteArtifact)
    assert db_qual_artifact.value == {"some": "data"}

    await delete_artifacts(artifact_filter)
    assert not await exists(artifact_filter)
    assert not await temp_file_storage.exists(db_qual_artifact.artifact.remote_artifact_location)


async def test_delete_many_artifacts():
    grandparent = new_artifact("gp", 1, json_serializer, storage=temp_file_storage)
    parent = new_artifact("p", 2, json_serializer)
    child = new_artifact("c", 3, json_serializer)
    grandchild = new_artifact("gc", 4, json_serializer, storage=temp_file_storage)

    artifacts = [grandparent, parent, child, grandchild]
    remote_artifacts = [grandparent, grandchild]

    # write artifacts
    await write_artifacts(artifacts)

    # create parent-child relationships
    await write_links(
        [
            (None, grandparent.artifact.node_id),
            (grandparent.artifact.node_id, parent.artifact.node_id),
            (parent.artifact.node_id, child.artifact.node_id),
            (child.artifact.node_id, grandchild.artifact.node_id),
        ]
    )

    artifact_filter = ArtifactFilter(
        child=NodeLinkFilter(
            descendant_of=grandparent.artifact.node_id,
            include_self=True,
        )
    )

    # check that the artifact exist
    assert await exists(artifact_filter)

    # delete artifacts
    await delete_artifacts(artifact_filter)

    # check that the artifact and its descendants were deleted
    assert not await exists(artifact_filter)

    # check that remote artifacts were deleted
    storage_locations = [qual.artifact.remote_artifact_location for qual in remote_artifacts]

    assert not any([await temp_file_storage.exists(location) for location in storage_locations])


async def test_read_child_artifacts():
    node = await write_one(OrmNode(), refresh_attributes=["node_id"])
    qual_artifacts = [
        new_artifact(str(i), i, json_serializer, parent_id=node.node_id) for i in range(10)
    ]
    quals = await write_artifacts(qual_artifacts)
    db_artifact_ids = {
        qual.artifact.node_id
        for qual in await read_artifacts(
            ArtifactFilter(child=NodeLinkFilter(child_of=[node.node_id]))
        )
    }
    assert db_artifact_ids == {q.artifact.node_id for q in quals}
