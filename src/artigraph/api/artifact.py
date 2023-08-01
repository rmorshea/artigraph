import asyncio
from typing import Any, Coroutine, Sequence, TypeVar

from sqlalchemy import select
from typing_extensions import TypeAlias

from artigraph.api.node import delete_nodes, read_descendants
from artigraph.db import current_session
from artigraph.orm.artifact import Artifact, DatabaseArtifact, RemoteArtifact
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
async def create_artifact(artifact: RemoteArtifact | DatabaseArtifact, value: Any) -> int:
    """Save the artifact to the database."""
    result = await create_artifacts([(artifact, value)])
    return result[0]


@syncable
async def read_artifact_by_id(artifact_id: int) -> QualifiedArtifact:
    """Load the artifact from the database."""
    stmt = select(Node.node_type).where(Node.node_id == artifact_id)
    async with current_session() as session:
        result = await session.execute(stmt)
        artifact_type = _get_artifact_type_by_name(result.scalar_one())
        stmt = select(artifact_type).where(artifact_type.node_id == artifact_id)
        result = await session.execute(stmt)
        artifact = result.scalar_one()

    if isinstance(artifact, RemoteArtifact):
        storage = get_storage_by_name(artifact.remote_artifact_storage)
        serializer = get_serialize_by_name(artifact.remote_artifact_serializer)
        value = serializer.deserialize(await storage.read(artifact.remote_artifact_location))
    elif isinstance(artifact, DatabaseArtifact):
        value = artifact.database_artifact_value
    else:
        msg = f"Unknown artifact type: {artifact}"
        raise RuntimeError(msg)

    return artifact, value


@syncable
async def create_artifacts(qualified_artifacts: Sequence[QualifiedArtifact]) -> Sequence[int]:
    """Save the artifacts to the database."""
    qualified_storage_artifacts: list[tuple[RemoteArtifact, Any]] = []
    database_artifacts: list[DatabaseArtifact] = []

    for artifact, value in qualified_artifacts:
        if isinstance(artifact, RemoteArtifact):
            qualified_storage_artifacts.append((artifact, value))
        else:
            artifact.database_artifact_value = value
            database_artifacts.append(artifact)

    # Save values to storage first
    remote_artifacts: list[RemoteArtifact] = []
    storage_create_coros: list[Coroutine[None, None, str]] = []
    for artifact, value in qualified_storage_artifacts:
        storage = get_storage_by_name(artifact.remote_artifact_storage)
        serializer = get_serialize_by_name(artifact.remote_artifact_serializer)
        remote_artifacts.append(artifact)
        storage_create_coros.append(storage.create(serializer.serialize(value)))

    storage_locations = await asyncio.gather(*storage_create_coros)

    for artifact, location in zip(remote_artifacts, storage_locations):
        artifact.remote_artifact_location = location

    # Save records in the database
    async with current_session() as session:
        session.add_all(database_artifacts)
        session.add_all(remote_artifacts)
        await session.commit()
        await asyncio.gather(*[session.refresh(a, ["node_id"]) for a, _ in qualified_artifacts])
        return [a.node_id for a, _ in qualified_artifacts]


@syncable
async def delete_artifacts(artifacts: Sequence[Artifact]) -> None:
    """Delete the artifacts from the database."""
    remote_artifacts: list[RemoteArtifact] = []
    for artifact in artifacts:
        if isinstance(artifact, RemoteArtifact):
            remote_artifacts.append(artifact)

    # Delete values from storage first
    storage_delete_coros: list[Coroutine[None, None, None]] = []
    for artifact in remote_artifacts:
        storage = get_storage_by_name(artifact.remote_artifact_storage)
        storage_delete_coros.append(storage.delete(artifact.remote_artifact_location))

    await asyncio.gather(*storage_delete_coros)
    await delete_nodes([a.node_id for a in artifacts])


@syncable
async def read_descendant_artifacts(root_node: Node) -> Sequence[QualifiedArtifact]:
    """Load the artifacts from the database."""
    storage_artifacts = await read_descendants(root_node, RemoteArtifact)
    database_artifacts = await read_descendants(root_node, DatabaseArtifact)

    qualified_artifacts: list[QualifiedArtifact] = [
        (a, a.database_artifact_value) for a in database_artifacts
    ]

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


def _get_artifact_type_by_name(name: str) -> type[Artifact]:
    """Get the artifact type by name."""
    for cls in Artifact.__subclasses__():
        if getattr(cls, "__mapper_args__", {}).get("polymorphic_identity") == name:
            return cls
    msg = f"Unknown artifact type {name}"
    raise ValueError(msg)
