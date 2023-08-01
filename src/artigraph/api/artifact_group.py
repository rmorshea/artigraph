from dataclasses import dataclass, fields
from typing import Any, Generic, Sequence, TypeVar

from typing_extensions import Self

from artigraph.api.artifact import (
    QualifiedArtifact,
    create_artifacts,
    group_artifacts_by_parent_id,
    read_artifacts,
)
from artigraph.api.node import read_children
from artigraph.orm.artifact import DatabaseArtifact, RemoteArtifact
from artigraph.orm.node import Node
from artigraph.orm.run import Run
from artigraph.serializer._core import Serializer, get_serializer_by_type
from artigraph.storage._core import Storage, get_storage_by_name
from artigraph.utils import UNDEFINED, syncable

T = TypeVar("T")
N = TypeVar("N", bound=Node)


ARTIFACT_GROUP_TYPES_BY_NAME: dict[str, type["ArtifactGroup"]] = {}


@dataclass
class Stored(Generic[T]):
    """A value within an ArtifactGroup that is saved in a storage backend."""

    value: T
    """The value of the artifact."""

    storage: Storage | str
    """The storage method for the artifact."""

    serializer: Serializer = UNDEFINED
    """The serializer used to serialize the artifact."""

    def __post_init__(self) -> None:
        if self.serializer is UNDEFINED:
            self.serializer = get_serializer_by_type(self.value)


@dataclass(frozen=True)
class ArtifactGroup:
    """A collection of artifacts that are saved together."""

    def __init_subclass__(cls) -> None:
        if cls.__name__ in ARTIFACT_GROUP_TYPES_BY_NAME:
            msg = f"An artifact group with the name {cls.__name__} already exists."
            raise ValueError(msg)
        ARTIFACT_GROUP_TYPES_BY_NAME[cls.__name__] = cls

    @syncable
    async def save(self, run: Run) -> None:
        """Save the artifacts to the database."""
        for group in await read_children(run, [DatabaseArtifact]):
            if group.artifact_label == "__group_type__":
                msg = f"Run {run.node_id} already has an artifact group."
                raise ValueError(msg)
        await create_artifacts(self._collect_qualified_artifacts(run))

    @classmethod
    @syncable
    async def load(cls, run: Run) -> Self:
        """Load the artifacts from the database."""
        artifacts = await read_artifacts(run)
        artifacts_by_parent_id = group_artifacts_by_parent_id(artifacts)

        for group, _ in artifacts_by_parent_id[run.node_id]:
            if isinstance(group, DatabaseArtifact) and group.artifact_label == "__group_type__":
                break
        else:
            msg = f"Run {run.node_id} does not have an artifact group."
            raise ValueError(msg)

        return await cls._from_artifacts(group, artifacts_by_parent_id)

    def _collect_qualified_artifacts(self, parent: Node) -> Sequence[QualifiedArtifact]:
        """Collect the records to be saved."""

        # creata a node for the group type
        group = DatabaseArtifact(
            node_parent_id=parent.node_id,
            artifact_label="__group_type__",
            database_artifact_value=self.__class__.__name__,
        )

        records: list[QualifiedArtifact] = [(group, group.database_artifact_value)]

        for f in fields(self):
            if not f.init:
                continue

            value = getattr(self, f.name)

            if isinstance(value, Stored):
                serializer = (
                    get_serializer_by_type(value.value)
                    if value.serializer is None
                    else value.serializer
                )
                storage = (
                    get_storage_by_name(value.storage)
                    if isinstance(value.storage, str)
                    else value.storage
                )
                st_artifact = RemoteArtifact(
                    node_parent_id=group.node_id,
                    artifact_label=f.name,
                    remote_artifact_storage=storage.name,
                    remote_artifact_serializer=serializer.name,
                )
                records.append((st_artifact, value.value))
            elif isinstance(value, ArtifactGroup):
                records.extend(value._collect_qualified_artifacts(group))
            else:
                db_artifact = DatabaseArtifact(
                    node_parent_id=group.node_id,
                    artifact_label=f.name,
                    database_artifact_value=value,
                )
                records.append((db_artifact, value))

        return records

    @classmethod
    async def _from_artifacts(
        cls,
        group: DatabaseArtifact,
        artifacts_by_parent_id: dict[int | None, list[QualifiedArtifact]],
    ) -> Self:
        """Load the artifacts from the database."""
        kwargs: dict[str, Any] = {}
        for artifact, value in artifacts_by_parent_id[group.node_id]:
            if (
                isinstance(artifact, DatabaseArtifact)
                and artifact.artifact_label == "__group_type__"
            ):
                other_cls = ARTIFACT_GROUP_TYPES_BY_NAME[artifact.database_artifact_value]
                kwargs[artifact.artifact_label] = other_cls._from_artifacts(
                    artifact, artifacts_by_parent_id
                )
            else:
                kwargs[artifact.artifact_label] = value
        return cls(**kwargs)
