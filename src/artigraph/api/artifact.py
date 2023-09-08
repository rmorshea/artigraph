from __future__ import annotations

from typing import ClassVar, Generic, Sequence, TypeVar

from typing_extensions import Self

from artigraph.api.node import (
    Node,
)
from artigraph.orm.artifact import OrmArtifact, OrmDatabaseArtifact, OrmRemoteArtifact
from artigraph.orm.link import OrmNodeLink
from artigraph.serializer import get_serializer_by_name
from artigraph.serializer.core import Serializer
from artigraph.storage.core import Storage, get_storage_by_name

T = TypeVar("T")
O = TypeVar("O", bound="OrmArtifact")  # noqa: E741


class Artifact(Node[OrmArtifact], Generic[T]):
    """A wrapper around an ORM artifact record."""

    orm_type: ClassVar[type[OrmArtifact]] = OrmArtifact

    value: T
    serializer: Serializer | None = None
    storage: Storage | None = None

    async def orm_dump(self) -> Sequence[OrmArtifact]:
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
                node_id=self.id,
                artifact_serializer=self.serializer.name,
                remote_artifact_storage=self.storage.name,
                remote_artifact_location=location,
            )
        else:
            artifact = OrmDatabaseArtifact(
                node_id=self.id,
                artifact_serializer=self.serializer.name,
                database_artifact_data=data,
            )

        return [artifact]

    @classmethod
    async def orm_load(
        cls,
        records: Sequence[OrmArtifact],
        related_records: dict[type[OrmNodeLink], Sequence[OrmNodeLink]],
    ) -> Sequence[Self]:
        parent_links, child_links = await cls.orm_load_parent_and_child_links(related_records)

        link_objs: list[Self] = []

        for r in records:
            serializer = get_serializer_by_name(r.artifact_serializer)

            if isinstance(r, OrmRemoteArtifact):
                storage = get_storage_by_name(r.remote_artifact_storage)
                data = await storage.read(r.remote_artifact_location)
            elif isinstance(r, OrmDatabaseArtifact):
                data = r.database_artifact_data
                storage = None
            else:
                msg = f"Unknown artifact type: {r}"
                raise RuntimeError(msg)

            link_objs.append(
                cls(
                    id=r.node_id,
                    parent_links=parent_links.get(r.node_id, ()),
                    child_links=child_links.get(r.node_id, ()),
                    value=serializer.deserialize(data),
                    serializer=serializer,
                    storage=storage,
                )
            )

        return link_objs
