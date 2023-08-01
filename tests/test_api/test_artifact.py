from artigraph.api.artifact import (
    create_artifact,
    create_artifacts,
    group_artifacts_by_parent_id,
    read_artifact_by_id,
    read_descendant_artifacts,
)
from artigraph.orm.artifact import DatabaseArtifact


async def test_create_and_read_database_artifact():
    """Test creating an artifact."""
    artifact = DatabaseArtifact(node_parent_id=None, artifact_label="test-label")
    artifact_id = await create_artifact(artifact, {"some": "data"})

    artifact, db_artifact_value = await read_artifact_by_id(artifact_id)
    assert isinstance(artifact, DatabaseArtifact)
    assert db_artifact_value == {"some": "data"}
