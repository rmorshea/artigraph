from __future__ import annotations

from typing import Any, Sequence, TypeVar, overload

from sqlalchemy import select
from typing_extensions import TypeAlias

from artigraph.api.node import (
    delete_node,
    is_node_type,
    read_child_nodes,
    read_descendant_nodes,
    read_node,
)
from artigraph.db import current_session, session_context
from artigraph.orm.artifact import BaseArtifact, DatabaseArtifact, RemoteArtifact
from artigraph.orm.node import Node
from artigraph.serializer import get_serializer_by_name
from artigraph.serializer.core import Serializer
from artigraph.storage.core import Storage, get_storage_by_name
from artigraph.utils import TaskBatch

T = TypeVar("T")
N = TypeVar("N", bound=Node)

QualifiedArtifact: TypeAlias = "tuple[RemoteArtifact | DatabaseArtifact, Any]"
"""An artifact with its value."""


@overload
def new_artifact(
    label: str,
    value: Any,
    serializer: Serializer,
    *,
    detail: str = ...,
    storage: Storage,
    parent_id: int | None = ...,
) -> tuple[RemoteArtifact, Any]:
    ...


@overload
def new_artifact(
    label: str,
    value: Any,
    serializer: Serializer,
    *,
    detail: str = ...,
    storage: Storage | None,
    parent_id: int | None = ...,
) -> tuple[DatabaseArtifact | RemoteArtifact, Any]:
    ...


@overload
def new_artifact(
    label: str,
    value: Any,
    serializer: Serializer,
    *,
    detail: str = ...,
    storage: None = ...,
    parent_id: int | None = ...,
) -> tuple[DatabaseArtifact, Any]:
    ...


def new_artifact(
    label: str,
    value: Any,
    serializer: Serializer,
    *,
    detail: str = "",
    storage: Storage | None = None,
    parent_id: int | None = None,
) -> QualifiedArtifact:
    """Construct a new artifact and its value"""
    return (
        DatabaseArtifact(
            node_parent_id=parent_id,
            artifact_label=label,
            artifact_serializer=serializer.name,
            artifact_detail=detail,
            database_artifact_value=serializer.serialize(value),
        )
        if storage is None
        else RemoteArtifact(
            node_parent_id=parent_id,
            artifact_label=label,
            artifact_detail=detail,
            artifact_serializer=serializer.name,
            remote_artifact_storage=storage.name,
        ),
        value,
    )


def group_artifacts_by_parent_id(
    qualified_artifacts: Sequence[QualifiedArtifact],
) -> dict[int | None, list[QualifiedArtifact]]:
    """Group artifacts by their parent id."""
    artifacts_by_parent_id: dict[int | None, list[QualifiedArtifact]] = {}
    for artifact, value in qualified_artifacts:
        artifacts_by_parent_id.setdefault(artifact.node_parent_id, []).append((artifact, value))
    return artifacts_by_parent_id


async def write_artifact(artifact: RemoteArtifact | DatabaseArtifact, value: Any) -> int:
    """Save the artifact to the database."""
    result = await write_artifacts([(artifact, value)])
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

    serializer = get_serializer_by_name(artifact.artifact_serializer)
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


async def write_artifacts(qualified_artifacts: Sequence[QualifiedArtifact]) -> Sequence[int]:
    """Save the artifacts to the database."""
    qualified_storage_artifacts: list[tuple[RemoteArtifact, Any]] = []
    database_artifacts: list[DatabaseArtifact] = []

    for artifact, value in qualified_artifacts:
        if isinstance(artifact, RemoteArtifact):
            qualified_storage_artifacts.append((artifact, value))
        else:
            serializer = get_serializer_by_name(artifact.artifact_serializer)
            artifact.database_artifact_value = serializer.serialize(value)
            database_artifacts.append(artifact)

    # Save values to storage first
    remote_artifacts: list[RemoteArtifact] = []
    storage_locations: TaskBatch[str] = TaskBatch()
    for artifact, value in qualified_storage_artifacts:
        storage = get_storage_by_name(artifact.remote_artifact_storage)
        serializer = get_serializer_by_name(artifact.artifact_serializer)
        remote_artifacts.append(artifact)
        storage_locations.add(storage.create, serializer.serialize(value))

    for artifact, location in zip(remote_artifacts, await storage_locations.gather()):
        artifact.remote_artifact_location = location

    # Save records in the database
    async with session_context() as session:
        session.add_all(database_artifacts + remote_artifacts)
        await session.commit()

        # We can't do this in asyncio.gather() because of issues with concurrent connections:
        # https://docs.sqlalchemy.org/en/20/errors.html#illegalstatechangeerror-and-concurrency-exceptions
        artifact_ids: list[int] = []
        for a, _ in qualified_artifacts:
            await session.refresh(a)
            artifact_ids.append(a.node_id)

        return artifact_ids


async def delete_artifact(artifact_id: int, *, descendants: bool = True) -> None:
    """Delete the artifacts from the database."""
    artifact = await read_node(artifact_id)

    remote_artifacts: list[RemoteArtifact] = []
    if isinstance(artifact, RemoteArtifact):
        remote_artifacts.append(artifact)
    if descendants:  # nocov (FIXME: actually covered but not detected)
        remote_artifacts.extend(await read_descendant_nodes(artifact_id, RemoteArtifact))

    remote_storage_deletions = TaskBatch()
    for ra in remote_artifacts:
        storage = get_storage_by_name(ra.remote_artifact_storage)
        remote_storage_deletions.add(storage.delete, ra.remote_artifact_location)
    await remote_storage_deletions.gather()

    await delete_node(artifact_id, descendants=descendants)


async def read_child_artifacts(root_node_id: int) -> Sequence[QualifiedArtifact]:
    return await _read_qualified_artifacts(await read_child_nodes(root_node_id))


async def read_descendant_artifacts(root_node_id: int) -> Sequence[QualifiedArtifact]:
    """Load the artifacts from the database."""
    return await _read_qualified_artifacts(await read_descendant_nodes(root_node_id))


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
            else get_serializer_by_name(d_artifact.artifact_serializer).deserialize(raw_value)
        )
        qualified_artifacts.append((d_artifact, value))

    # Load values from storage
    storage_data: TaskBatch[Any] = TaskBatch()
    for r_artifact in remote_artifacts:
        storage = get_storage_by_name(r_artifact.remote_artifact_storage)
        storage_data.add(storage.read, r_artifact.remote_artifact_location)

    for r_artifact, data in zip(remote_artifacts, await storage_data.gather()):
        serializer = get_serializer_by_name(r_artifact.artifact_serializer)
        qualified_artifacts.append((r_artifact, serializer.deserialize(data)))

    return qualified_artifacts


def _get_artifact_type_by_name(name: str) -> type[BaseArtifact]:
    """Get the artifact type by name."""
    for cls in BaseArtifact.__subclasses__():
        if cls.polymorphic_identity == name:
            return cls
    msg = f"Unknown artifact type {name}"  # nocov
    raise ValueError(msg)  # nocov
