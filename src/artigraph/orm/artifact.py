from typing import Any, ClassVar

from sqlalchemy import JSON
from sqlalchemy.orm import Mapped, mapped_column

from artigraph.orm.node import Node


class Artifact(Node):
    """A base class for artifacts."""

    artifact_label: Mapped[str] = mapped_column(use_existing_column=True, nullable=True)
    """A label for the artifact."""


class RemoteArtifact(Artifact):
    """An artifact saved via a storage backend."""

    __mapper_args__: ClassVar[dict[str, Any]] = {"polymorphic_identity": "storage_artifact"}

    remote_artifact_location: Mapped[str] = mapped_column(init=False, nullable=True)
    """A string describing where the artifact is stored."""

    remote_artifact_serializer: Mapped[str] = mapped_column(nullable=True)
    """The name of the serializer used to serialize the artifact."""

    remote_artifact_storage: Mapped[str] = mapped_column(nullable=True)
    """The name of the storage method for the artifact."""


class DatabaseArtifact(Artifact):
    """An artifact saved directly in the database."""

    __mapper_args__: ClassVar[dict[str, Any]] = {"polymorphic_identity": "database_artifact"}

    database_artifact_value: Mapped[Any] = mapped_column(JSON, nullable=True)
    """The data of the artifact."""
