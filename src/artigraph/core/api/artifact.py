from __future__ import annotations

from dataclasses import dataclass
from typing import Any, ClassVar, Generic, Sequence, TypeVar

from artigraph.core.api.node import Node
from artigraph.core.orm.artifact import OrmArtifact, OrmDatabaseArtifact, OrmRemoteArtifact
from artigraph.core.serializer.base import (
    Serializer,
    get_serializer_by_name,
    get_serializer_by_type,
)
from artigraph.core.storage.base import Storage, get_storage_by_name

T = TypeVar("T")
O = TypeVar("O", bound="OrmArtifact")


async def load_deserialized_artifact_value(obj: OrmArtifact) -> Any:
    """Load the value of an artifact from its ORM record."""

    if isinstance(obj, OrmRemoteArtifact):
        storage = get_storage_by_name(obj.remote_artifact_storage)
        data = await storage.read(obj.remote_artifact_location)
    elif isinstance(obj, OrmDatabaseArtifact):
        data = obj.database_artifact_data
        storage = None
    else:  # nocov
        msg = f"Unknown artifact type: {obj}"
        raise RuntimeError(msg)

    if data is not None and obj.artifact_serializer is not None:
        data = get_serializer_by_name(obj.artifact_serializer).deserialize(data)

    return data


class Artifact(Node[OrmArtifact], Generic[T]):
    """A wrapper around an ORM artifact record."""

    graph_orm_type: ClassVar[type[OrmArtifact]] = OrmArtifact

    value: T
    serializer: Serializer | None = None
    storage: Storage | None = None

    async def graph_dump_self(self) -> OrmArtifact:
        if self.serializer is not None:
            serializer_name = self.serializer.name
            if self.value is None:
                data = None
            else:
                data = self.serializer.serialize(self.value)
        elif isinstance(self.value, bytes):
            serializer_name = None
            data = self.value
        else:
            serializer = get_serializer_by_type(type(self.value))[0]
            data = serializer.serialize(self.value)
            serializer_name = serializer.name

        if data is not None and self.storage is not None:
            location = await self.storage.create(data)
            artifact = OrmRemoteArtifact(
                id=self.graph_id,
                artifact_serializer=serializer_name,
                remote_artifact_storage=self.storage.name,
                remote_artifact_location=location,
            )
        else:
            artifact = OrmDatabaseArtifact(
                id=self.graph_id,
                artifact_serializer=serializer_name,
                database_artifact_data=data,
            )

        return artifact

    @classmethod
    async def _graph_load_extra_kwargs(cls, self_record: OrmArtifact) -> dict[str, Any]:
        return {
            **await super()._graph_load_extra_kwargs(self_record),
            "value": await load_deserialized_artifact_value(self_record),
            "serializer": (
                get_serializer_by_name(self_record.artifact_serializer)
                if self_record.artifact_serializer
                else None
            ),
            "storage": (
                get_storage_by_name(self_record.remote_artifact_storage)
                if isinstance(self_record, OrmRemoteArtifact)
                else None
            ),
        }


@dataclass(frozen=True)
class SaveSpec:
    """Information about how to save an artifact."""

    serializers: Sequence[Serializer] = ()
    """The serializers to try when saving the artifact."""

    storage: Storage | None = None
    """The storage to use when saving the artifact."""

    def is_empty(self) -> bool:
        """Return whether this save spec is empty."""
        return not self.serializers and self.storage is None

    def create_artifact(self, value: T, *, strict: bool = False) -> Artifact[T]:
        """Create an artifact from a value."""
        if isinstance(value, bytes):
            return Artifact(value=value, serializer=None, storage=self.storage)

        for s in self.serializers:
            if isinstance(value, s.types):
                return Artifact(value=value, serializer=s, storage=self.storage)

        if strict:
            if not self.serializers:
                msg = f"No serializers specified for {value!r}"
                raise ValueError(msg)
            allowed_types = ", ".join([t.__name__ for s in self.serializers for t in s.types])
            msg = f"Expected {allowed_types} - got {value!r}"
            raise TypeError(msg)

        serializer = get_serializer_by_type(type(value))[0]
        return Artifact(value=value, serializer=serializer, storage=self.storage)
