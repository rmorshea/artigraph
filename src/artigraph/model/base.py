from __future__ import annotations

from typing import Any, ClassVar, Sequence, TypedDict, cast
from urllib.parse import quote, unquote

from sqlalchemy import select
from typing_extensions import Self

import artigraph
from artigraph.api.artifact import (
    QualifiedArtifact,
    create_artifacts,
    group_artifacts_by_parent_id,
    new_artifact,
    new_database_artifact,
    read_artifact_by_id,
    read_descendant_artifacts,
)
from artigraph.api.node import (
    create_nodes,
    create_parent_child_relationships,
    read_node,
    with_current_node_id,
)
from artigraph.db import current_session, session_context
from artigraph.orm.artifact import BaseArtifact, DatabaseArtifact
from artigraph.serializer import Serializer
from artigraph.serializer.json import json_serializer, json_sorted_serializer
from artigraph.storage import Storage
from artigraph.utils import SessionBatch

MODEL_TYPES_BY_NAME: dict[str, type[BaseModel]] = {}


def get_model_type_by_name(name: str) -> type[BaseModel]:
    """Get an artifact model type by its name."""
    try:
        return MODEL_TYPES_BY_NAME[name]
    except KeyError:
        msg = f"Unknown artifact model type {name!r}"
        raise ValueError(msg) from None


@with_current_node_id
async def create_node_models(node_id: int, *, models: dict[str, BaseModel]) -> dict[str, int]:
    """Add an artifact to the span and return its ID"""
    ids: dict[str, int] = {}
    for k, v in models.items():
        ids[k] = await create_model(k, v, parent_id=node_id)
    return ids


@with_current_node_id
async def read_node_models(span_id: int, *, labels: Sequence[str] = ()) -> dict[str, BaseModel]:
    """Load all artifacts for this span."""
    artifact_models: dict[str, BaseModel] = {}
    async with current_session() as session:
        cmd = select(BaseArtifact.node_id, BaseArtifact.artifact_label).where(
            BaseArtifact.node_parent_id == span_id
        )
        if labels:
            cmd = cmd.where(BaseArtifact.artifact_label.in_(labels))
        result = await session.execute(cmd)
        node_ids_and_labels = list(result.all())

    if not node_ids_and_labels:
        return artifact_models

    node_ids, artifact_labels = zip(*node_ids_and_labels)
    for label, model in zip(
        artifact_labels,
        await SessionBatch().map(read_model, node_ids).gather(),
    ):
        artifact_models[label] = model

    return artifact_models


async def create_model(label: str, model: BaseModel, parent_id: int | None = None) -> int:
    parent_node = None if parent_id is None else await read_node(parent_id)

    models_by_path = _get_model_paths(model)
    nodes_by_path = {
        path: _get_model_artifact(_get_model_label_from_path(path), model)
        for path, model in models_by_path.items()
    }

    root_node = nodes_by_path[""]
    root_node.node_parent_id = parent_id
    root_node.artifact_label = label

    async with session_context(expire_on_commit=False):
        await create_nodes(
            list(nodes_by_path.values()),
            refresh_attributes=["node_id"],
        )
        await create_parent_child_relationships(
            [
                (nodes_by_path.get(_get_model_parent_path(path), parent_node), node)
                for path, node in nodes_by_path.items()
            ]
        )
        await create_artifacts(
            [
                fn
                for path, model in models_by_path.items()
                for fn in _model_field_artifacts(model, nodes_by_path[path])
            ]
        )

    return root_node.node_id


async def read_model(node_id: int) -> BaseModel:
    """Load the artifact model from the database."""
    root_qaul_artifact = await read_artifact_by_id(node_id)
    model_info = _get_artifact_model_info(root_qaul_artifact)
    if model_info is None:
        msg = f"Node {node_id} is not an artifact model."
        raise ValueError(msg)
    root_node, model_name, model_version, _ = model_info
    cls = get_model_type_by_name(model_name)

    artifacts = await read_descendant_artifacts(root_node.node_id)
    artifacts_by_parent_id = group_artifacts_by_parent_id(artifacts)
    return await _load_from_artifacts(cls, root_node, model_version, artifacts_by_parent_id)


class ModelConfig(TypedDict, total=False):
    """Configure the behavior of an artifact model class."""

    default_field_serializer: Serializer | None
    """The default serializer for fields on this model"""

    default_field_storage: Storage | None
    """The default storage for fields on this model"""


class FieldConfig(TypedDict, total=False):
    """The metadata for an artifact model field."""

    serializer: Serializer | None
    """The serializer for the artifact model field."""

    storage: Storage | None
    """The storage for the artifact model field."""


class BaseModel:
    """An interface for all modeled artifacts."""

    model_version: ClassVar[int] = 1
    """The version of the artifact model."""

    model_config: ClassVar[ModelConfig] = ModelConfig()
    """The configuration for the artifact model."""

    @classmethod
    def model_migrate(cls, version: int, kwargs: dict[str, Any], /) -> Self:
        """Migrate the artifact model to a new version."""
        raise NotImplementedError()

    def model_data(self) -> dict[str, tuple[Any, FieldConfig]]:
        """The data for the artifact model."""
        raise NotImplementedError()

    def __init_subclass__(cls, **kwargs: Any):
        name = cls.__name__
        if name in MODEL_TYPES_BY_NAME:
            msg = f"Artifact model named {name!r} already exists"
            raise RuntimeError(msg)
        MODEL_TYPES_BY_NAME[name] = cls


class ModelMetadata(TypedDict):
    """The metadata for an artifact model."""

    artigraph_version: str
    """The version of Artigraph used to generate the model"""


def _get_artifact_model_info(
    qual_artifact: QualifiedArtifact,
) -> None | tuple[DatabaseArtifact, str, int, ModelMetadata]:
    artifact, value = qual_artifact
    if not isinstance(value, dict) or not value.get("artigraph_version"):
        return None
    artifact = cast(DatabaseArtifact, artifact)
    metadata = cast(ModelMetadata, value)
    model_name, class_version_str = artifact.artifact_detail.split("-")
    model_version = int(class_version_str.lstrip("v"))
    return artifact, model_name, model_version, metadata


def _get_model_artifact(label: str, model: BaseModel) -> DatabaseArtifact:
    return new_database_artifact(
        label,
        ModelMetadata(artigraph_version=artigraph.__version__),
        serializer=json_sorted_serializer,
        detail=_get_model_detail(model),
    )[0]


def _get_model_paths(model: BaseModel, path: str = "") -> dict[str, BaseModel]:
    """Get the paths of all artifact models in the tree."""
    paths: dict[str, BaseModel] = {path: model}
    for label, (value, _) in model.model_data().items():
        if not isinstance(value, BaseModel):
            continue
        escaped_key = quote(label)
        for child_path, child_model in _get_model_paths(value, f"{path}/{escaped_key}").items():
            paths[child_path] = child_model
    return paths


def _model_field_artifacts(
    model: BaseModel, parent: DatabaseArtifact
) -> Sequence[QualifiedArtifact]:
    artifacts: list[QualifiedArtifact] = []
    for label, (value, config) in model.model_data().items():
        if isinstance(value, BaseModel):
            continue

        storage = config.get("storage") or model.model_config.get("default_field_storage")
        serializer = (
            config.get("serializer")
            or model.model_config.get("default_field_serializer")
            or json_serializer
        )

        artifacts.append(
            new_artifact(
                label,
                value,
                storage=storage,
                serializer=serializer,
                parent_id=parent.node_id,
                detail=_get_model_detail(model),
            )
        )
    return artifacts


async def _load_from_artifacts(
    cls: type[BaseModel],
    model_node: DatabaseArtifact,
    model_version: int,
    artifacts_by_parent_id: dict[int | None, list[QualifiedArtifact]],
) -> Self:
    """Load the artifacts from the database."""
    kwargs: dict[str, Any] = {}
    for qual_artifact in artifacts_by_parent_id[model_node.node_id]:
        maybe_model_info = _get_artifact_model_info(qual_artifact)
        if maybe_model_info is not None:
            node, model_name, model_version, _ = maybe_model_info
            other_cls = MODEL_TYPES_BY_NAME[model_name]
            kwargs[node.artifact_label] = await _load_from_artifacts(
                other_cls,
                node,
                model_version,
                artifacts_by_parent_id,
            )
        else:
            node, value = qual_artifact
            kwargs[node.artifact_label] = value

    if model_version != cls.model_version:
        return cls.model_migrate(model_version, kwargs)
    else:
        return cls(**kwargs)


def _get_model_detail(model: BaseModel) -> str:
    """Get the name and version of the artifact model."""
    return f"{type(model).__name__}-v{model.model_version}"


def _get_model_parent_path(path: str) -> str:
    """Get the path of the parent artifact model."""
    return "__parent_of_root__" if path == "" else path.rsplit("/", 1)[0]


def _get_model_label_from_path(path: str) -> str:
    """Get the name of the artifact model field from a path."""
    return unquote(path.rsplit("/", 1)[-1])
