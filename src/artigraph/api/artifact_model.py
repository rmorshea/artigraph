from dataclasses import dataclass, fields
from typing import Any, ClassVar, Generic, Sequence, TypedDict, TypeGuard, TypeVar

from typing_extensions import Self

from artigraph.api.artifact import (
    QualifiedArtifact,
    create_artifacts,
    group_artifacts_by_parent_id,
    read_descendant_artifacts,
)
from artigraph.api.node import is_node_type, read_children, read_node
from artigraph.db import current_session
from artigraph.orm.artifact import DatabaseArtifact, RemoteArtifact
from artigraph.orm.node import Node
from artigraph.orm.run import Run
from artigraph.serializer._core import Serializer, get_serialize_by_name, get_serializer_by_type
from artigraph.storage._core import Storage, get_storage_by_name
from artigraph.utils import syncable

T = TypeVar("T")
N = TypeVar("N", bound=Node)

_ARTIFACT_MODEL_LABEL = "__artifact_model__"
ARTIFACT_MODEL_TYPES_BY_NAME: dict[str, type["ArtifactModel"]] = {}


@dataclass(frozen=True)
class RemoteModel(Generic[T]):
    """A value within an ArtifactModel that is saved in a storage backend."""

    value: T
    """The value of the artifact."""

    storage: Storage
    """The storage method for the artifact."""

    serializer: Serializer[T] | None = None
    """The serializer used to serialize the artifact."""


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
    async def save(self, node: Node | None = None) -> DatabaseArtifact:
        """Save the artifacts to the database."""
        if node is not None:
            for child in await read_children(node.node_id, DatabaseArtifact):
                if _is_artifact_model_node(node):
                    msg = f"Run {child.node_id} already has an artifact group."
                    raise ValueError(msg)
        async with current_session():
            model_node, nodes = await self._collect_qualified_artifacts(node)
            await create_artifacts(nodes)
        return model_node

    @classmethod
    @syncable
    async def load(cls, node: Node | int) -> Self:
        """Load the artifacts from the database."""
        if isinstance(node, int):
            node = await read_node(node)

        if is_node_type(node, DatabaseArtifact):
            node = await read_node(node.node_id, DatabaseArtifact)
            if node.artifact_label != _ARTIFACT_MODEL_LABEL:
                msg = f"Node {node} is not an artifact model node."
                raise ValueError(msg)
            model_node = node
        elif is_node_type(node, Run):
            for model_node in await read_children(node.node_id, DatabaseArtifact):
                if model_node.artifact_label == _ARTIFACT_MODEL_LABEL:
                    break
            else:
                msg = f"Run {node} does not have an artifact model."
                raise ValueError(msg)
        else:
            msg = f"Node {node} is not a run or artifact model node."
            raise ValueError(msg)

        artifacts = await read_descendant_artifacts(model_node.node_id)
        artifacts_by_parent_id = group_artifacts_by_parent_id(artifacts)
        return await cls._load_from_artifacts(model_node, artifacts_by_parent_id)

    async def _collect_qualified_artifacts(
        self, parent: Node | None
    ) -> tuple[DatabaseArtifact, Sequence[QualifiedArtifact]]:
        """Collect the records to be saved."""

        # creata a node for the model
        model_node = _create_artifact_model_node(parent, self)
        async with current_session() as session:
            session.add(model_node)
            await session.commit()
            await session.refresh(model_node)

        records: list[QualifiedArtifact] = []

        for f in fields(self):
            if not f.init:
                continue

            value = getattr(self, f.name)

            if isinstance(value, RemoteModel):
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
                _, inner_records = await value._collect_qualified_artifacts(model_node)
                records.extend(inner_records)
            else:
                db_artifact = DatabaseArtifact(
                    node_parent_id=model_node.node_id,
                    artifact_label=f.name,
                    database_artifact_value=value,
                )
                records.append((db_artifact, value))

        return model_node, records

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
            elif is_node_type(node, RemoteArtifact):
                storage = get_storage_by_name(node.remote_artifact_storage)
                serializer = get_serialize_by_name(node.remote_artifact_serializer)
                kwargs[node.artifact_label] = RemoteModel(
                    storage=storage, serializer=serializer, value=value
                )
            else:
                kwargs[node.artifact_label] = value

        if model_node.database_artifact_value != cls.metadata():
            kwargs = cls.migrate(model_node.database_artifact_value, kwargs)

        return cls(**kwargs)


def _create_artifact_model_node(parent: Node | None, model: ArtifactModel) -> DatabaseArtifact:
    """Create a node for an artifact model."""
    return DatabaseArtifact(
        node_parent_id=None if parent is None else parent.node_id,
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
