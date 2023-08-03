from typing import Any, ClassVar

from sqlalchemy import UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from artigraph.orm.node import Node


class BaseArtifact(Node):
    """A base class for artifacts."""

    __table_args__ = (UniqueConstraint("node_parent_id", "artifact_label"),)
    __mapper_args__: ClassVar[dict[str, Any]] = {"polymorphic_abstract": True}

    artifact_label: Mapped[str] = mapped_column(use_existing_column=True, nullable=True)
    """A label for the artifact."""

    artifact_serializer: Mapped[str] = mapped_column(nullable=True)
    """The name of the serializer used to serialize the artifact."""


class RemoteArtifact(BaseArtifact):
    """An artifact saved via a storage backend."""

    polymorphic_identity = "remote_artifact"
    __mapper_args__: ClassVar[dict[str, Any]] = {"polymorphic_identity": polymorphic_identity}

    remote_artifact_storage: Mapped[str] = mapped_column(nullable=True)
    """The name of the storage method for the artifact."""

    remote_artifact_location: Mapped[str] = mapped_column(init=False, nullable=True)
    """A string describing where the artifact is stored."""


class DatabaseArtifact(BaseArtifact):
    """An artifact saved directly in the database."""

    polymorphic_identity = "database_artifact"
    __mapper_args__: ClassVar[dict[str, Any]] = {"polymorphic_identity": polymorphic_identity}

    database_artifact_value: Mapped[bytes | None] = mapped_column(default=None)
    """The data of the artifact."""
