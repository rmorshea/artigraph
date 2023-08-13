from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Generic, Sequence, TypeVar, overload

from sqlalchemy import inspect
from typing_extensions import TypeAlias

from artigraph.api.filter import ArtifactFilter, Filter
from artigraph.api.node import (
    delete_nodes,
    read_node,
    read_node_or_none,
    read_nodes,
)
from artigraph.db import current_session
from artigraph.orm.artifact import BaseArtifact, DatabaseArtifact, RemoteArtifact
from artigraph.orm.node import Node
from artigraph.serializer import get_serializer_by_name
from artigraph.serializer.core import Serializer
from artigraph.storage.core import Storage, get_storage_by_name
from artigraph.utils import TaskBatch

V = TypeVar("V")
A = TypeVar("A", bound="RemoteArtifact | DatabaseArtifact")
N = TypeVar("N", bound=Node)


@dataclass(frozen=True)
class QualifiedArtifact(Generic[A, V]):
    """An artifact and its value."""

    artifact: A
    """The database record"""

    value: V
    """The deserialized value"""


AnyQualifiedArtifact: TypeAlias = """
    QualifiedArtifact[RemoteArtifact | DatabaseArtifact, Any]
    | QualifiedArtifact[DatabaseArtifact, Any]
    | QualifiedArtifact[RemoteArtifact, Any]
"""  # noqa: F722
"""A convenience type for any qualified artifact."""


@overload
def new_artifact(
    label: str,
    value: V,
    serializer: Serializer,
    *,
    storage: Storage,
    parent_id: int | None = ...,
) -> QualifiedArtifact[RemoteArtifact, V]:
    ...


@overload
def new_artifact(
    label: str,
    value: V,
    serializer: Serializer,
    *,
    storage: Storage | None,
    parent_id: int | None = ...,
) -> QualifiedArtifact[RemoteArtifact | DatabaseArtifact, V]:
    ...


@overload
def new_artifact(
    label: str,
    value: V,
    serializer: Serializer,
    *,
    storage: None = ...,
    parent_id: int | None = ...,
) -> QualifiedArtifact[DatabaseArtifact, V]:
    ...


def new_artifact(
    label: str,
    value: V,
    serializer: Serializer,
    *,
    storage: Storage | None = None,
    parent_id: int | None = None,
) -> (
    QualifiedArtifact[RemoteArtifact | DatabaseArtifact, V]
    | QualifiedArtifact[RemoteArtifact, V]
    | QualifiedArtifact[DatabaseArtifact, V]
):
    """Construct a new artifact and its value"""
    return QualifiedArtifact(
        artifact=(
            DatabaseArtifact(
                node_parent_id=parent_id,
                artifact_label=label,
                artifact_serializer=serializer.name,
                database_artifact_value=None,
            )
            if storage is None
            else RemoteArtifact(
                node_parent_id=parent_id,
                artifact_label=label,
                artifact_serializer=serializer.name,
                remote_artifact_storage=storage.name,
            )
        ),
        value=value,
    )


def group_artifacts_by_parent_id(
    qualified_artifacts: Sequence[AnyQualifiedArtifact],
) -> dict[int | None, list[AnyQualifiedArtifact]]:
    """Group artifacts by their parent id."""
    artifacts_by_parent_id: dict[int | None, list[AnyQualifiedArtifact]] = {}
    for qualart in qualified_artifacts:
        artifacts_by_parent_id.setdefault(qualart.artifact.node_parent_id, []).append(
            QualifiedArtifact(qualart.artifact, qualart.value)
        )
    return artifacts_by_parent_id


async def delete_artifacts(artifact_filter: ArtifactFilter[Any] | Filter) -> None:
    """Delete the artifacts from the database."""
    artifacts = await read_nodes(artifact_filter)

    remote_storage_deletions = TaskBatch()
    for a in artifacts:
        if isinstance(a, RemoteArtifact):
            storage = get_storage_by_name(a.remote_artifact_storage)
            remote_storage_deletions.add(storage.delete, a.remote_artifact_location)

    await remote_storage_deletions.gather()
    await delete_nodes(artifact_filter)


async def read_artifact(artifact_filter: ArtifactFilter[A] | Filter) -> QualifiedArtifact[A, Any]:
    """Load the artifact from the database."""
    return (await _load_qualified_artifacts([await read_node(artifact_filter)]))[0]


async def read_artifact_or_none(
    artifact_filter: ArtifactFilter[A] | Filter,
) -> QualifiedArtifact[A, Any] | None:
    """Load the artifact from the database, or return None if it does not exist."""
    artifact_node = await read_node_or_none(artifact_filter)
    return (await _load_qualified_artifacts([artifact_node]))[0] if artifact_node else None


async def read_artifacts(
    artifact_filter: ArtifactFilter[A] | Filter,
) -> Sequence[QualifiedArtifact[A, Any]]:
    """Load the artifact from the database."""
    artifact_nodes = await read_nodes(artifact_filter)
    return await _load_qualified_artifacts(artifact_nodes)


async def write_artifact(qualified_artifact: AnyQualifiedArtifact) -> AnyQualifiedArtifact:
    """Save the artifact to the database."""
    return (await write_artifacts([qualified_artifact]))[0]


async def write_artifacts(
    qualified_artifacts: Sequence[AnyQualifiedArtifact],
) -> Sequence[AnyQualifiedArtifact]:
    """Save the artifacts to the database."""
    qualified_storage_artifacts: list[tuple[RemoteArtifact, Any]] = []
    database_artifacts: list[DatabaseArtifact] = []

    for qual in qualified_artifacts:
        if isinstance(qual.artifact, RemoteArtifact):
            qualified_storage_artifacts.append((qual.artifact, qual.value))
        elif isinstance(qual.artifact, DatabaseArtifact):
            if qual.value is not None:
                serializer = get_serializer_by_name(qual.artifact.artifact_serializer)
                qual.artifact.database_artifact_value = serializer.serialize(qual.value)
                database_artifacts.append(qual.artifact)
            else:
                qual.artifact.database_artifact_value = None
        else:  # nocov
            msg = f"Unknown artifact type: {qual.artifact}"
            raise RuntimeError(msg)

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
    async with current_session() as session:
        session.add_all(database_artifacts + remote_artifacts)
        await session.commit()

        # We can't do this in asyncio.gather() because of issues with concurrent connections:
        # https://docs.sqlalchemy.org/en/20/errors.html#illegalstatechangeerror-and-concurrency-exceptions
        artifact_ids: list[int] = []
        for qual in qualified_artifacts:
            if inspect(qual.artifact).persistent:
                await session.refresh(qual.artifact)
            artifact_ids.append(qual.artifact.node_id)

        return qualified_artifacts


async def _load_qualified_artifacts(artifact_nodes: Sequence[A]) -> list[QualifiedArtifact[A, Any]]:
    artifact_nodes_and_bytes: list[tuple[BaseArtifact, bytes | None]] = []

    remote_artifact_nodes: list[RemoteArtifact] = []
    for node in artifact_nodes:
        if isinstance(node, DatabaseArtifact):
            artifact_nodes_and_bytes.append((node, node.database_artifact_value))
        elif isinstance(node, RemoteArtifact):
            remote_artifact_nodes.append(node)
        else:  # nocov
            msg = f"Unknown artifact type: {node}"
            raise RuntimeError(msg)

    remote_artifact_values = TaskBatch()
    for node in remote_artifact_nodes:
        storage = get_storage_by_name(node.remote_artifact_storage)
        remote_artifact_values.add(storage.read, node.remote_artifact_location)

    for node, value in zip(remote_artifact_nodes, await remote_artifact_values.gather()):
        artifact_nodes_and_bytes.append((node, value))

    qualified_artifacts: list[QualifiedArtifact] = []
    for node, value in artifact_nodes_and_bytes:
        serializer = get_serializer_by_name(node.artifact_serializer)
        qualified_artifacts.append(
            QualifiedArtifact(node, value if value is None else serializer.deserialize(value))
        )

    return qualified_artifacts
