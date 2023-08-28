from __future__ import annotations

from typing import ClassVar, Generic, Sequence, TypeVar

from typing_extensions import Self

from artigraph.api.node import (
    Node,
)
from artigraph.orm.artifact import OrmArtifact, OrmDatabaseArtifact, OrmRemoteArtifact
from artigraph.serializer import get_serializer_by_name
from artigraph.serializer.core import Serializer
from artigraph.storage.core import Storage, get_storage_by_name

T = TypeVar("T")
O = TypeVar("O", bound="OrmArtifact")  # noqa: E741


class Artifact(Node[O], Generic[O, T]):
    """A wrapper around an ORM artifact record."""

    orm_type: ClassVar[type[O]] = OrmArtifact

    value: T
    serializer: Serializer | None = None
    storage: Storage | None = None

    async def to_orms(self) -> Sequence[O]:
        if self.serializer is not None:
            data = self.serializer.serialize(self.value)
        elif isinstance(self.value, bytes):
            data = self.value
        else:
            msg = f"Must specify a serializer for non-bytes artifact: {self.value}"
            raise ValueError(msg)

        if self.storage is not None:
            location = await self.storage.create(data)
            artifact = OrmRemoteArtifact(
                node_id=self.node_id,
                artifact_serializer=self.serializer.name,
                remote_artifact_storage=self.storage,
                remote_artifact_location=location,
            )
        else:
            artifact = OrmDatabaseArtifact(
                node_id=self.node_id,
                artifact_serializer=self.serializer.name,
                database_artifact_data=data,
            )

        return [artifact]

    @classmethod
    async def from_orm(cls, orm: O) -> Self:
        serializer = get_serializer_by_name(orm.artifact_serializer)

        if isinstance(orm, OrmRemoteArtifact):
            storage = get_storage_by_name(orm.remote_artifact_storage)
            data = await storage.read(orm.remote_artifact_location)
        elif isinstance(orm, OrmDatabaseArtifact):
            data = orm.database_artifact_data
            storage = None
        else:
            msg = f"Unknown artifact type: {orm}"
            raise RuntimeError(msg)

        return cls(
            value=serializer.deserialize(data),
            serializer=serializer,
            storage=storage,
            node_id=orm.node_id,
            api_orm=orm,
        )
