import asyncio
from typing import Any, Coroutine, Sequence, TypeVar

from sqlalchemy import select
from typing_extensions import TypeAlias

from artigraph.api.node import delete_nodes, is_node_type, read_children, read_descendants
from artigraph.db import current_session
from artigraph.orm.artifact import BaseArtifact, DatabaseArtifact, RemoteArtifact
from artigraph.orm.node import Node
from artigraph.serializer import get_serialize_by_name
from artigraph.storage.core import get_storage_by_name

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


async def create_artifact(artifact: RemoteArtifact | DatabaseArtifact, value: Any) -> int:
    """Save the artifact to the database."""
    result = await create_artifacts([(artifact, value)])
    return result[0]


async def read_artifact_by_id(artifact_id: int) -> QualifiedArtifact:
    """Load the artifact from the database."""
    cmd = select(Node.node_type).where(Node.node_id == artifact_id)
    async with current_session() as session:
        result = await session.execute(cmd)
        artifact_type = _get_artifact_type_by_name(result.scalar_one())
        cmd = select(artifact_type).where(artifact_type.node_id == artifact_id)
        result = await session.execute(cmd)
        artifact = result.scalar_one()

    serializer = get_serialize_by_name(artifact.artifact_serializer)
    if isinstance(artifact, RemoteArtifact):
        storage = get_storage_by_name(artifact.remote_artifact_storage)
        value = serializer.deserialize(await storage.read(artifact.remote_artifact_location))
    elif isinstance(artifact, DatabaseArtifact):
        value = (
            None
            if artifact.database_artifact_value is None
            else serializer.deserialize(artifact.database_artifact_value)
        )
    else:  # nocov
        msg = f"Unknown artifact type: {artifact}"
        raise RuntimeError(msg)

    return artifact, value


async def create_artifacts(qualified_artifacts: Sequence[QualifiedArtifact]) -> Sequence[int]:
    """Save the artifacts to the database."""
    qualified_storage_artifacts: list[tuple[RemoteArtifact, Any]] = []
    database_artifacts: list[DatabaseArtifact] = []

    for artifact, value in qualified_artifacts:
        if isinstance(artifact, RemoteArtifact):
            qualified_storage_artifacts.append((artifact, value))
        else:
            serializer = get_serialize_by_name(artifact.artifact_serializer)
            artifact.database_artifact_value = serializer.serialize(value)
            database_artifacts.append(artifact)

    # Save values to storage first
    remote_artifacts: list[RemoteArtifact] = []
    storage_create_coros: list[Coroutine[None, None, str]] = []
    for artifact, value in qualified_storage_artifacts:
        storage = get_storage_by_name(artifact.remote_artifact_storage)
        serializer = get_serialize_by_name(artifact.artifact_serializer)
        remote_artifacts.append(artifact)
        storage_create_coros.append(storage.create(serializer.serialize(value)))

    storage_locations = await asyncio.gather(*storage_create_coros)

    for artifact, location in zip(remote_artifacts, storage_locations):
        artifact.remote_artifact_location = location

    # Save records in the database
    async with current_session() as session:
        session.add_all(database_artifacts + remote_artifacts)
        await session.commit()

        # We can't do this in asyncio.gather() because of issues with concurrent connections:
        # https://docs.sqlalchemy.org/en/20/errors.html#illegalstatechangeerror-and-concurrency-exceptions
        artifact_ids: list[int] = []
        for a, _ in qualified_artifacts:
            await session.refresh(a, ["node_id"])
            artifact_ids.append(a.node_id)

        return artifact_ids


async def delete_artifacts(artifacts: Sequence[BaseArtifact]) -> None:
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


async def read_child_artifacts(root_node_id: int) -> Sequence[QualifiedArtifact]:
    return await _read_qualified_artifacts(await read_children(root_node_id))


async def read_descendant_artifacts(root_node_id: int) -> Sequence[QualifiedArtifact]:
    """Load the artifacts from the database."""
    return await _read_qualified_artifacts(await read_descendants(root_node_id))


async def _read_qualified_artifacts(all_artifacts: Sequence[Any]) -> Sequence[QualifiedArtifact]:
    remote_artifacts: list[RemoteArtifact] = []
    database_artifacts: list[DatabaseArtifact] = []
    for a in all_artifacts:
        if is_node_type(a, RemoteArtifact):
            remote_artifacts.append(a)
        elif is_node_type(a, DatabaseArtifact):
            database_artifacts.append(a)
        else:  # nocov
            msg = f"Unknown artifact type: {a}"
            raise RuntimeError(msg)

    qualified_artifacts: list[QualifiedArtifact] = []

    for d_artifact in database_artifacts:
        raw_value = d_artifact.database_artifact_value
        value = (
            None
            if raw_value is None
            else get_serialize_by_name(d_artifact.artifact_serializer).deserialize(raw_value)
        )
        qualified_artifacts.append((d_artifact, value))

    # Load values from storage
    storage_read_coros: list[Coroutine[None, None, Any]] = []
    for r_artifact in remote_artifacts:
        storage = get_storage_by_name(r_artifact.remote_artifact_storage)
        storage_read_coros.append(storage.read(r_artifact.remote_artifact_location))

    storage_data = await asyncio.gather(*storage_read_coros)

    for r_artifact, data in zip(remote_artifacts, storage_data):
        serializer = get_serialize_by_name(r_artifact.artifact_serializer)
        qualified_artifacts.append((r_artifact, serializer.deserialize(data)))

    return qualified_artifacts


def _get_artifact_type_by_name(name: str) -> type[BaseArtifact]:
    """Get the artifact type by name."""
    for cls in BaseArtifact.__subclasses__():
        if cls.polymorphic_identity == name:
            return cls
    msg = f"Unknown artifact type {name}"  # nocov
    raise ValueError(msg)  # nocov
