import asyncio
from typing import Any, Coroutine, Sequence, TypeVar

from typing_extensions import TypeAlias

from artigraph.api.node import read_recursive_children
from artigraph.db import enter_session
from artigraph.orm.artifact import DatabaseArtifact, StorageArtifact
from artigraph.orm.node import Node
from artigraph.serializer._core import get_serialize_by_name
from artigraph.storage._core import get_storage_by_name
from artigraph.utils import syncable

T = TypeVar("T")
N = TypeVar("N", bound=Node)

QualifiedArtifact: TypeAlias = tuple[StorageArtifact | DatabaseArtifact, Any]
"""An artifact with its value."""


def group_artifacts_by_parent_id(
    qualified_artifacts: Sequence[QualifiedArtifact],
) -> dict[int, Sequence[QualifiedArtifact]]:
    """Group artifacts by their parent id."""
    artifacts_by_parent_id: dict[int, list[QualifiedArtifact]] = {}
    for artifact, value in qualified_artifacts:
        artifacts_by_parent_id.setdefault(artifact.parent_id, []).append((artifact, value))
    return artifacts_by_parent_id


@syncable
async def create_artifacts(qualified_artifacts: Sequence[QualifiedArtifact]) -> None:
    """Save the artifacts to the database."""
    qualified_storage_artifacts: list[tuple[StorageArtifact, Any]] = []
    database_artifacts: list[DatabaseArtifact] = []

    for artifact, value in qualified_artifacts:
        if isinstance(artifact, StorageArtifact):
            qualified_storage_artifacts.append((artifact, value))
        else:
            database_artifacts.append(artifact)

    # Save values to storage first
    storage_artifacts: list[StorageArtifact] = []
    storage_create_coros: list[Coroutine[None, None, str]] = []
    for artifact, value in qualified_storage_artifacts:
        storage = get_storage_by_name(artifact.storage)
        serializer = get_serialize_by_name(artifact.serializer)
        storage_artifacts.append(artifact)
        storage_create_coros.append(storage.create(serializer.serialize(value)))

    storage_locations = await asyncio.gather(*storage_create_coros)

    for artifact, location in zip(storage_artifacts, storage_locations):
        artifact.location = location

    # Save records in the database
    async with enter_session() as session:
        session.add_all(database_artifacts)
        session.add_all(storage_artifacts)
        await session.commit()


@syncable
async def read_artifacts(root_node: Node) -> Sequence[QualifiedArtifact]:
    """Load the artifacts from the database."""
    artifacts = await read_recursive_children(root_node, [DatabaseArtifact, StorageArtifact])

    storage_artifacts: list[StorageArtifact] = []
    qualified_artifacts: list[QualifiedArtifact] = []
    for a in artifacts:
        if isinstance(a, StorageArtifact):
            storage_artifacts.append(a)
        else:
            qualified_artifacts.append((a, a.value))

    # Load values from storage
    storage_read_coros: list[Coroutine[None, None, Any]] = []
    for artifact in storage_artifacts:
        storage = get_storage_by_name(artifact.storage)
        storage_read_coros.append(storage.read(artifact.location))

    storage_data = await asyncio.gather(*storage_read_coros)

    for artifact, data in zip(storage_artifacts, storage_data):
        serializer = get_serialize_by_name(artifact.serializer)
        qualified_artifacts.append((artifact, serializer.deserialize(data)))

    return qualified_artifacts
