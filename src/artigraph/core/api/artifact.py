from __future__ import annotations

from typing import Any, ClassVar, Generic, Sequence, TypeVar

from typing_extensions import Self

from artigraph.core.api.node import (
    Node,
)
from artigraph.core.orm.artifact import OrmArtifact, OrmDatabaseArtifact, OrmRemoteArtifact
from artigraph.core.orm.link import OrmNodeLink
from artigraph.core.serializer import get_serializer_by_name
from artigraph.core.serializer.base import Serializer
from artigraph.core.storage.base import Storage, get_storage_by_name

T = TypeVar("T")
O = TypeVar("O", bound="OrmArtifact")  # noqa: E741


async def load_deserialized_artifact_value(obj: OrmArtifact) -> Any:
    """Load the value of an artifact from its ORM record."""
    serializer = get_serializer_by_name(obj.artifact_serializer)

    if isinstance(obj, OrmRemoteArtifact):
        storage = get_storage_by_name(obj.remote_artifact_storage)
        data = await storage.read(obj.remote_artifact_location)
    elif isinstance(obj, OrmDatabaseArtifact):
        data = obj.database_artifact_data
        storage = None
    else:
        msg = f"Unknown artifact type: {obj}"
        raise RuntimeError(msg)

    return serializer.deserialize(data)


class Artifact(Node[OrmArtifact], Generic[T]):
    """A wrapper around an ORM artifact record."""

    graph_orm_type: ClassVar[type[OrmArtifact]] = OrmArtifact

    value: T
    serializer: Serializer | None = None
    storage: Storage | None = None

    async def graph_dump_self(self) -> OrmArtifact:
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
                remote_artifact_storage=self.storage.name,
                remote_artifact_location=location,
            )
        else:
            artifact = OrmDatabaseArtifact(
                node_id=self.node_id,
                artifact_serializer=self.serializer.name,
                database_artifact_data=data,
            )

        return artifact

    @classmethod
    async def graph_load(
        cls,
        self_records: Sequence[OrmDatabaseArtifact | OrmRemoteArtifact],
        related_records: dict[type[OrmNodeLink], Sequence[OrmNodeLink]],
    ) -> Sequence[Self]:
        parent_links, child_links = await cls.graph_load_parent_and_child_links(related_records)
        return [
            cls(
                node_id=r.node_id,
                parent_links=parent_links.get(r.node_id, ()),
                child_links=child_links.get(r.node_id, ()),
                value=await load_deserialized_artifact_value(r),
                serializer=get_serializer_by_name(r.artifact_serializer),
                storage=(
                    get_storage_by_name(r.remote_artifact_storage)
                    if isinstance(r, OrmRemoteArtifact)
                    else None
                ),
            )
            for r in self_records
        ]
