from dataclasses import dataclass, fields
from typing import Any, ClassVar, Generic, Sequence, TypedDict, TypeGuard, TypeVar

from typing_extensions import Self

from artigraph.api.artifact import (
    QualifiedArtifact,
    create_artifacts,
    group_artifacts_by_parent_id,
    read_descendant_artifacts,
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

_ARTIFACT_MODEL_LABEL = "__artifact_model__"
ARTIFACT_MODEL_TYPES_BY_NAME: dict[str, type["ArtifactModel"]] = {}


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
class ArtifactModel:
    """A collection of artifacts that are saved together."""

    version: ClassVar[int] = 1

    @classmethod
    def metadata(cls) -> "ArtifactModelMetadata":
        """Get the metadata for the artifact model."""
        return ArtifactModelMetadata(
            model_type=cls.__name__,
            model_version=cls.version,
        )

    @classmethod
    def migrate(
        cls,
        metadata: "ArtifactModelMetadata",  # noqa: ARG003
        data: dict[str, Any],
    ) -> dict[str, Any]:
        """Migrate the artifact model to a new version."""
        return data

    def __init_subclass__(cls) -> None:
        if cls.__name__ in ARTIFACT_MODEL_TYPES_BY_NAME:
            msg = f"An artifact model with the name {cls.__name__} already exists."
            raise ValueError(msg)
        ARTIFACT_MODEL_TYPES_BY_NAME[cls.__name__] = cls

    @syncable
    async def save(self, run: Run) -> None:
        """Save the artifacts to the database."""
        for node in await read_children(run, [DatabaseArtifact]):
            if _is_artifact_model_node(node):
                msg = f"Run {run.node_id} already has an artifact group."
                raise ValueError(msg)
        await create_artifacts(self._collect_qualified_artifacts(run))

    @classmethod
    @syncable
    async def load(cls, run: Run) -> Self:
        """Load the artifacts from the database."""
        artifacts = await read_descendant_artifacts(run)
        artifacts_by_parent_id = group_artifacts_by_parent_id(artifacts)

        for node, _ in artifacts_by_parent_id[run.node_id]:
            if _is_artifact_model_node(node):
                model_node = node
                break
        else:
            msg = f"Run {run.node_id} does not have an artifact group."
            raise ValueError(msg)

        return await cls._load_from_artifacts(model_node, artifacts_by_parent_id)

    def _collect_qualified_artifacts(self, parent: Node) -> Sequence[QualifiedArtifact]:
        """Collect the records to be saved."""

        # creata a node for the model
        model_node = _create_artifact_model_node(parent, self)

        records: list[QualifiedArtifact] = [(model_node, model_node.database_artifact_value)]

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
                    node_parent_id=model_node.node_id,
                    artifact_label=f.name,
                    remote_artifact_storage=storage.name,
                    remote_artifact_serializer=serializer.name,
                )
                records.append((st_artifact, value.value))
            elif isinstance(value, ArtifactModel):
                records.extend(value._collect_qualified_artifacts(model_node))
            else:
                db_artifact = DatabaseArtifact(
                    node_parent_id=model_node.node_id,
                    artifact_label=f.name,
                    database_artifact_value=value,
                )
                records.append((db_artifact, value))

        return records

    @classmethod
    async def _load_from_artifacts(
        cls,
        model_node: DatabaseArtifact,
        artifacts_by_parent_id: dict[int | None, list[QualifiedArtifact]],
    ) -> Self:
        """Load the artifacts from the database."""
        kwargs: dict[str, Any] = {}
        for node, value in artifacts_by_parent_id[model_node.node_id]:
            if _is_artifact_model_node(node):
                other_cls = ARTIFACT_MODEL_TYPES_BY_NAME[node.database_artifact_value]
                kwargs[node.artifact_label] = other_cls._load_from_artifacts(
                    node, artifacts_by_parent_id
                )
            else:
                kwargs[node.artifact_label] = value

        if model_node.database_artifact_value != cls.metadata():
            kwargs = cls.migrate(model_node.database_artifact_value, kwargs)

        return cls(**kwargs)


def _create_artifact_model_node(parent: DatabaseArtifact, model: ArtifactModel) -> DatabaseArtifact:
    """Create a node for an artifact model."""
    return DatabaseArtifact(
        node_parent_id=parent.node_id,
        artifact_label=_ARTIFACT_MODEL_LABEL,
        database_artifact_value=model.metadata(),
    )


def _is_artifact_model_node(node: Node) -> TypeGuard[DatabaseArtifact]:
    """Check if a node describes an artifact model."""
    return isinstance(node, DatabaseArtifact) and node.artifact_label == _ARTIFACT_MODEL_LABEL


class ArtifactModelMetadata(TypedDict):
    """The metadata for an artifact model."""

    model_type: str
    """The type of artifact model."""

    model_version: int
    """The version of the artifact model."""
