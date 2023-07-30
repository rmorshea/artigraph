from dataclasses import dataclass, fields
from typing import Any, Generic, Sequence, TypeVar

from typing_extensions import Self

from artigraph.api.artifact import (
    QualifiedArtifact,
    create_artifacts,
    group_artifacts_by_parent_id,
    read_artifacts,
)
from artigraph.api.node import read_direct_children
from artigraph.orm.artifact import DatabaseArtifact, StorageArtifact
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
    async def create(self, run: Run) -> None:
        """Save the artifacts to the database."""
        for group in await read_direct_children(run, [DatabaseArtifact]):
            if group.label == "__group_type__":
                msg = f"Run {run.id} already has an artifact group."
                raise ValueError(msg)
        await create_artifacts(self._collect_qualified_artifacts(run))

    @classmethod
    @syncable
    async def read(cls, run: Run) -> "ArtifactGroup[T]":
        """Load the artifacts from the database."""
        artifacts = await read_artifacts(run)
        artifacts_by_parent_id = group_artifacts_by_parent_id(artifacts)

        for group, _ in artifacts_by_parent_id[run.id]:
            if group.label == "__group_type__":
                break
        else:
            msg = f"Run {run.id} does not have an artifact group."
            raise ValueError(msg)

        return await cls._from_artifacts(group, artifacts_by_parent_id)

    def _collect_qualified_artifacts(self, parent: Node) -> Sequence[QualifiedArtifact]:
        """Collect the records to be saved."""

        # creata a node for the group type
        group = DatabaseArtifact(
            parent_id=parent.id,
            label="__group_type__",
            value=self.__class__.__name__,
        )

        records: list[QualifiedArtifact] = [(group, group.value)]

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
                artifact = StorageArtifact(
                    parent_id=group.id,
                    label=f.name,
                    storage=storage.name,
                    serializer=serializer.name,
                )
                records.append((artifact, value.value))
            elif isinstance(value, ArtifactGroup):
                records.extend(value._collect_qualified_artifacts(group))
            else:
                artifact = DatabaseArtifact(
                    parent_id=group.id,
                    label=f.name,
                    value=value,
                )
                records.append(artifact)

        return records

    @classmethod
    @syncable
    async def _from_artifacts(
        cls,
        group: DatabaseArtifact,
        artifacts_by_parent_id: dict[int, Sequence[QualifiedArtifact]],
    ) -> Self:
        """Load the artifacts from the database."""
        kwargs: dict[str, Any] = {}
        for artifact, value in artifacts_by_parent_id[group.id]:
            if isinstance(artifact, DatabaseArtifact) and artifact.label == "__group_type__":
                other_cls = ARTIFACT_GROUP_TYPES_BY_NAME[artifact.value]
                kwargs[artifact.label] = other_cls._from_artifacts(artifact, artifacts_by_parent_id)
            else:
                kwargs[artifact.label] = value
        return cls(**kwargs)
