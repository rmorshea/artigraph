import asyncio
from typing import Any, Coroutine, Sequence, TypeVar

from typing_extensions import TypeAlias

from artigraph.api.node import read_descendants
from artigraph.db import current_session
from artigraph.orm.artifact import DatabaseArtifact, RemoteArtifact
from artigraph.orm.node import Node
from artigraph.serializer._core import get_serialize_by_name
from artigraph.storage._core import get_storage_by_name
from artigraph.utils import syncable

T = TypeVar("T")
N = TypeVar("N", bound=Node)

QualifiedArtifact: TypeAlias = tuple[RemoteArtifact | DatabaseArtifact, Any]
"""An artifact with its value."""


def group_artifacts_by_parent_id(
    qualified_artifacts: Sequence[QualifiedArtifact],
) -> dict[int | None, list[QualifiedArtifact]]:
    """Group artifacts by their parent id."""
    artifacts_by_parent_id: dict[int | None, list[QualifiedArtifact]] = {}
    for artifact, value in qualified_artifacts:
        artifacts_by_parent_id.setdefault(artifact.node_parent_id, []).append((artifact, value))
    return artifacts_by_parent_id


@syncable
async def create_artifacts(qualified_artifacts: Sequence[QualifiedArtifact]) -> None:
    """Save the artifacts to the database."""
    qualified_storage_artifacts: list[tuple[RemoteArtifact, Any]] = []
    database_artifacts: list[DatabaseArtifact] = []

    for artifact, value in qualified_artifacts:
        if isinstance(artifact, RemoteArtifact):
            qualified_storage_artifacts.append((artifact, value))
        else:
            database_artifacts.append(artifact)

    # Save values to storage first
    storage_artifacts: list[RemoteArtifact] = []
    storage_create_coros: list[Coroutine[None, None, str]] = []
    for artifact, value in qualified_storage_artifacts:
        storage = get_storage_by_name(artifact.remote_artifact_storage)
        serializer = get_serialize_by_name(artifact.remote_artifact_serializer)
        storage_artifacts.append(artifact)
        storage_create_coros.append(storage.create(serializer.serialize(value)))

    storage_locations = await asyncio.gather(*storage_create_coros)

    for artifact, location in zip(storage_artifacts, storage_locations):
        artifact.remote_artifact_location = location

    # Save records in the database
    async with current_session() as session:
        session.add_all(database_artifacts)
        session.add_all(storage_artifacts)
        await session.commit()


@syncable
async def read_artifacts(root_node: Node) -> Sequence[QualifiedArtifact]:
    """Load the artifacts from the database."""
    artifacts = await read_descendants(root_node, [DatabaseArtifact, RemoteArtifact])

    storage_artifacts: list[RemoteArtifact] = []
    qualified_artifacts: list[QualifiedArtifact] = []
    for a in artifacts:
        if isinstance(a, RemoteArtifact):
            storage_artifacts.append(a)
        else:
            qualified_artifacts.append((a, a.value))

    # Load values from storage
    storage_read_coros: list[Coroutine[None, None, Any]] = []
    for artifact in storage_artifacts:
        storage = get_storage_by_name(artifact.remote_artifact_storage)
        storage_read_coros.append(storage.read(artifact.remote_artifact_location))

    storage_data = await asyncio.gather(*storage_read_coros)

    for artifact, data in zip(storage_artifacts, storage_data):
        serializer = get_serialize_by_name(artifact.remote_artifact_serializer)
        qualified_artifacts.append((artifact, serializer.deserialize(data)))

    return qualified_artifacts
