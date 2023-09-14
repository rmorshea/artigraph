from __future__ import annotations

from typing import Any, ClassVar, Generic, TypeVar

from artigraph.core.api.node import Node
from artigraph.core.orm.artifact import OrmArtifact, OrmDatabaseArtifact, OrmRemoteArtifact
from artigraph.core.serializer.base import Serializer, get_serializer_by_name
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
        else:  # nocov
            msg = f"Must specify a serializer for non-bytes artifact: {self.value}"
            raise ValueError(msg)

        if data is not None and self.storage is not None:
            location = await self.storage.create(data)
            artifact = OrmRemoteArtifact(
                node_id=self.node_id,
                artifact_serializer=serializer_name,
                remote_artifact_storage=self.storage.name,
                remote_artifact_location=location,
            )
        else:
            artifact = OrmDatabaseArtifact(
                node_id=self.node_id,
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
