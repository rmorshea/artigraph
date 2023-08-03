from collections.abc import Mapping, Sequence
from dataclasses import Field, field, fields
from typing import Any, ClassVar, Literal, TypedDict, TypeVar
from urllib.parse import quote, unquote

from typing_extensions import Self, TypeGuard, dataclass_transform

from artigraph.api.artifact import (
    QualifiedArtifact,
    create_artifacts,
    group_artifacts_by_parent_id,
    read_artifact_by_id,
    read_descendant_artifacts,
)
from artigraph.api.node import create_nodes, create_parent_child_relationships
from artigraph.db import current_session
from artigraph.orm.artifact import DatabaseArtifact, RemoteArtifact
from artigraph.serializer import Serializer, get_serializer_by_type
from artigraph.serializer.json import json_serializer
from artigraph.storage import Storage
from artigraph.utils import syncable

T = TypeVar("T")
A = TypeVar("A", bound="ArtifactModel | ArtifactMapping | ArtifactSequence")

ARTIFACT_MODEL_TYPES_BY_NAME: dict[str, type["ArtifactModel"]] = {}


def artifact_field(
    *, serializer: Serializer[T] | None = None, storage: Storage | None = None, **kwargs: Any
) -> Field:
    """A dataclass field with a serializer and storage."""
    metadata = kwargs.get("metadata", {})
    metadata = {
        "artifact_model_field_config": ArtifactFieldConfig(
            serializer=serializer,
            storage=storage,
        ),
        **metadata,
    }
    return field(**kwargs)


class ArtifactModelConfig(TypedDict, total=False):
    """Configure the behavior of an artifact model class."""

    default_field_serializer: Serializer | None
    """The default serializer for fields on this model"""

    default_field_storage: Storage | None
    """The default storage for fields on this model"""


class ArtifactFieldConfig(TypedDict, total=False):
    """The metadata for an artifact model field."""

    serializer: Serializer | None
    """The serializer for the artifact model field."""

    storage: Storage | None
    """The storage for the artifact model field."""


@dataclass_transform(field_specifiers=(artifact_field,))
class ArtifactModel:
    """A collection of artifacts that are saved together."""

    model_version: ClassVar[int] = 1
    """The version of the artifact model."""

    model_config: ClassVar[ArtifactModelConfig] = ArtifactModelConfig()
    """The configuration for the artifact model."""

    def __init_subclass__(
        cls,
        *,
        version: int,
        config: "ArtifactModelConfig | None" = None,
    ) -> None:
        if cls.__name__ in ARTIFACT_MODEL_TYPES_BY_NAME:
            msg = f"Artifact model named {cls.__name__!r} already exists"
            raise RuntimeError(msg)
        cls.model_version = version
        cls.model_config = config or ArtifactModelConfig()

    @classmethod
    def migrate(
        cls,
        metadata: "_ArtifactModelMetadata",  # noqa: ARG003
        data: dict[str, Any],
    ) -> dict[str, Any]:
        """Migrate the artifact model to a new version."""
        return data

    @syncable
    async def save(self, label: str, parent_id: int | None = None) -> int:
        """Save the artifact model to the database."""
        models_by_path = _get_model_paths(self)
        nodes_by_path = {
            path: model._model_node(_get_model_label_from_path(path))
            for path, model in models_by_path.items()
        }

        root_node = nodes_by_path[""]
        root_node.node_parent_id = parent_id
        root_node.artifact_label = label

        async with current_session():
            await create_nodes(
                list(nodes_by_path.values()),
                refresh_attributes=["node_id"],
            )
            await create_parent_child_relationships(
                [
                    (nodes_by_path[_get_model_parent_path(path)], node)
                    for path, node in nodes_by_path.items()
                ]
            )
            await create_artifacts(
                [
                    fn
                    for path, model in models_by_path.items()
                    for fn in model._model_field_artifacts(nodes_by_path[path])
                ]
            )

        return root_node.node_id

    @classmethod
    @syncable
    async def load(cls, node_id: int) -> Self:
        """Load the artifact model from the database."""
        root_node, root_node_metadata = read_artifact_by_id(node_id)
        if not _is_artifact_model_metadata(root_node_metadata):
            msg = f"Node {node_id} is not an artifact model."
            raise ValueError(msg)

        artifacts = await read_descendant_artifacts(root_node.node_id)
        artifacts_by_parent_id = group_artifacts_by_parent_id(artifacts)
        return await cls._load_from_artifacts(root_node, root_node_metadata, artifacts_by_parent_id)

    def _model_children(self) -> dict[str, "ArtifactModel"]:
        children: dict[str, ArtifactModel] = {}
        for f in fields(self):
            if not f.init:
                continue
            v = getattr(self, f.name)
            if not isinstance(v, ArtifactModel):
                continue
            if "artifact_model_field_config" in f.metadata:
                msg = (
                    f"Attribute {f.name!r} of {self} is a model field "
                    f"but contains an artifact model {v}."
                )
                raise ValueError(msg)
            children[f.name] = v
        return children

    def _model_node(self, label: str) -> DatabaseArtifact:
        return DatabaseArtifact(
            node_parent_id=None,
            artifact_label=label,
            artifact_serializer=json_serializer.name,
            database_artifact_value=json_serializer.serialize(
                _ArtifactModelMetadata(
                    __is_artifact_model__=True,
                    model_type=self.__class__.__name__,
                    model_version=self.__class__.model_version,
                )
            ),
        )

    def _model_field_artifacts(self, parent: DatabaseArtifact) -> Sequence[QualifiedArtifact]:
        artifacts: list[QualifiedArtifact] = []
        for f in fields(self):
            if not f.init:
                continue
            v = getattr(self, f.name)
            if isinstance(v, ArtifactModel):
                continue

            value = getattr(self, f.name)
            field_config: ArtifactFieldConfig = f.metadata.get(
                "artifact_model_field_config", ArtifactFieldConfig()
            )

            serializer = (
                field_config.get("serializer")
                or self.model_config.get("default_field_serializer")
                or get_serializer_by_type(value)
            )
            storage = field_config.get("storage") or self.model_config.get("default_field_storage")

            if storage is None:
                artifacts.append(
                    (
                        DatabaseArtifact(
                            parent_node_id=parent.node_id,
                            artifact_label=f.name,
                            artifact_serializer=serializer.name,
                        ),
                        value,
                    )
                )
            else:
                artifacts.append(
                    (
                        RemoteArtifact(
                            parent_node_id=parent.node_id,
                            artifact_label=f.name,
                            artifact_serializer=serializer.name,
                            remote_artifact_storage=storage.name,
                        ),
                        value,
                    )
                )
        return artifacts

    @classmethod
    async def _load_from_artifacts(
        cls,
        model_node: DatabaseArtifact,
        model_metadata: "_ArtifactModelMetadata",
        artifacts_by_parent_id: dict[int | None, list[QualifiedArtifact]],
    ) -> Self:
        """Load the artifacts from the database."""
        kwargs: dict[str, Any] = {}
        for node, value in artifacts_by_parent_id[model_node.node_id]:
            if _is_artifact_model_metadata(value):
                metadata: _ArtifactModelMetadata = node.database_artifact_value
                other_cls = ARTIFACT_MODEL_TYPES_BY_NAME[metadata["model_type"]]
                kwargs[node.artifact_label] = await other_cls._load_from_artifacts(
                    node,
                    metadata,
                    artifacts_by_parent_id,
                )
            else:
                kwargs[node.artifact_label] = value

        if model_metadata["model_version"] != cls.model_version:
            kwargs = cls.migrate(model_node.database_artifact_value, kwargs)

        return cls(**kwargs)


class ArtifactMapping(ArtifactModel, Mapping[str, A], version=1):
    """A mapping whose values are other artifact models"""

    def __init__(self, *args: dict[str, A], **kwargs: A):
        self._data = dict(*args, **kwargs)

    def _artifact_model_children(self) -> dict[str, "ArtifactModel"]:
        return {k: v for k, v in self.items() if isinstance(v, ArtifactModel)}


class ArtifactSequence(Sequence[A], ArtifactModel, version=1):
    """A sequence whose values are other artifact models"""

    def __init__(self, *args: A) -> None:
        self._data = tuple(*args)

    def _artifact_model_children(self) -> dict[str, "ArtifactModel"]:
        return {str(i): v for i, v in enumerate(self) if isinstance(v, ArtifactModel)}


class _ArtifactModelMetadata(TypedDict):
    """The metadata for an artifact model."""

    __is_artifact_model__: Literal[True]
    """Marker that this is an artifact model."""

    model_type: str
    """The type of the artifact model."""

    model_version: int
    """The version of the artifact model."""


def _is_artifact_model_metadata(value: Any) -> TypeGuard[_ArtifactModelMetadata]:
    """Check if the value is artifact model metadata."""
    return isinstance(value, dict) and value.get("__is_artifact_model__")


def _get_model_paths(model: ArtifactModel, path: str = "") -> dict[str, ArtifactModel]:
    """Get the paths of all artifact models in the tree."""
    paths: dict[str, ArtifactModel] = {path: model}
    for key, child in model._model_children().items():
        escaped_key = quote(key)
        for child_path, child_model in _get_model_paths(child, f"{path}/{escaped_key}").items():
            paths[child_path] = child_model
    return paths


def _get_model_parent_path(path: str) -> str:
    """Get the path of the parent artifact model."""
    return path.rsplit("/", 1)[0]


def _get_model_label_from_path(path: str) -> str:
    """Get the name of the artifact model field from a path."""
    return unquote(path.rsplit("/", 1)[-1])
