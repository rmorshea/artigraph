from typing import Any, ClassVar

from sqlalchemy import JSON, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from artigraph.orm.node import Node


class Artifact(Node):
    """A base class for artifacts."""

    __table_args__ = (UniqueConstraint("node_parent_id", "artifact_label"),)

    artifact_label: Mapped[str] = mapped_column(use_existing_column=True, nullable=True)
    """A label for the artifact."""


class RemoteArtifact(Artifact):
    """An artifact saved via a storage backend."""

    polymorphic_identity = "remote_artifact"
    __mapper_args__: ClassVar[dict[str, Any]] = {"polymorphic_identity": polymorphic_identity}

    remote_artifact_serializer: Mapped[str] = mapped_column(nullable=True)
    """The name of the serializer used to serialize the artifact."""

    remote_artifact_storage: Mapped[str] = mapped_column(nullable=True)
    """The name of the storage method for the artifact."""

    remote_artifact_location: Mapped[str] = mapped_column(init=False, nullable=True)
    """A string describing where the artifact is stored."""


class DatabaseArtifact(Artifact):
    """An artifact saved directly in the database."""

    polymorphic_identity = "database_artifact"
    __mapper_args__: ClassVar[dict[str, Any]] = {"polymorphic_identity": polymorphic_identity}

    database_artifact_value: Mapped[Any] = mapped_column(JSON, nullable=True, default=None)
    """The data of the artifact."""
