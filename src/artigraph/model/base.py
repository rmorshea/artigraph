from __future__ import annotations

from contextvars import ContextVar
from typing import Any, ClassVar, Sequence, TypedDict, cast

from sqlalchemy import select
from typing_extensions import Self, TypeAlias

import artigraph
from artigraph.api.artifact import (
    QualifiedArtifact,
    group_artifacts_by_parent_id,
    new_artifact,
    read_artifact_by_id,
    read_descendant_artifacts,
    write_artifacts,
)
from artigraph.api.node import (
    read_node,
    with_current_node_id,
    write_nodes,
    write_parent_child_relationships,
)
from artigraph.db import current_session, session_context
from artigraph.orm import BaseArtifact, DatabaseArtifact, Node
from artigraph.serializer import Serializer
from artigraph.serializer.json import json_serializer, json_sorted_serializer
from artigraph.storage import Storage
from artigraph.utils import SessionBatch

ModelData: TypeAlias = "dict[str, tuple[Any, FieldConfig]]"

MODEL_TYPES_BY_NAME: dict[str, type[BaseModel]] = {}
MODELED_TYPES: dict[type[Any], type[BaseModel]] = {}

# useful in an interactive context (e.g. IPython/Jupyter)
ALLOW_MODEL_TYPE_OVERWRITES = ContextVar("ALLOW_MODEL_TYPE_OVERWRITES", default=False)


def try_convert_value_to_modeled_type(value: Any) -> BaseModel | Any:
    """Try to convert a value to a modeled type."""
    modeled_type = MODELED_TYPES.get(type(value))
    if modeled_type is not None:
        return modeled_type(value)  # type: ignore
    return value


def get_model_type_by_name(name: str) -> type[BaseModel]:
    """Get an artifact model type by its name."""
    try:
        return MODEL_TYPES_BY_NAME[name]
    except KeyError:
        msg = f"Unknown artifact model type {name!r}"
        raise ValueError(msg) from None


@with_current_node_id
async def write_child_models(node_id: int, models: dict[str, BaseModel]) -> dict[str, int]:
    """Add artifacts that are linked to the given node"""
    ids: dict[str, int] = {}
    for k, v in models.items():
        ids[k] = await write_model(k, v, parent_id=node_id)
    return ids


@with_current_node_id
async def read_child_models(node_id: int, labels: Sequence[str] = ()) -> dict[str, BaseModel]:
    """Read artifacts that are directly linked to the given node"""
    artifact_models: dict[str, BaseModel] = {}
    async with current_session() as session:
        cmd = select(BaseArtifact.node_id, BaseArtifact.artifact_label).where(
            BaseArtifact.node_parent_id == node_id
        )
        if labels:  # nocov (FIXME: this is covered but not detected)
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


async def write_model(label: str, model: BaseModel, parent_id: int | None = None) -> int:
    parent_node = None if parent_id is None else await read_node(parent_id)

    models_and_data_by_path = _get_models_and_data_by_paths(model)
    nodes_by_path = {
        path: _get_model_artifact(_get_model_label_from_path(path), model)
        for path, (model, _) in models_and_data_by_path.items()
    }

    root_node = nodes_by_path[""]
    root_node.node_parent_id = parent_id
    root_node.artifact_label = label

    async with session_context(expire_on_commit=False):
        # write the model nodes
        await write_nodes(list(nodes_by_path.values()), refresh_attributes=["node_id"])

        # create the model node relationships
        await write_parent_child_relationships(
            _get_parent_child_id_pairs_from_nodes_by_path(nodes_by_path, parent_node)
        )

        # attach model fields to the model nodes
        await write_artifacts(
            _get_field_artifacts_from_models_and_nodes_by_path(
                models_and_data_by_path, nodes_by_path
            )
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


class FieldConfig(TypedDict, total=False):
    """The metadata for an artifact model field."""

    serializer: Serializer
    """The serializer for the artifact model field."""

    storage: Storage
    """The storage for the artifact model field."""

    annotation: Any
    """The type annotation for the artifact model field."""


class BaseModel:
    """An interface for all modeled artifacts."""

    model_version: ClassVar[int] = 1
    """The version of the artifact model."""

    @classmethod
    def model_init(cls, version: int, data: dict[str, Any], /) -> Self:
        """Initialize the artifact model, migrating it if necessary."""
        raise NotImplementedError()  # nocov

    def model_data(self) -> dict[str, tuple[Any, FieldConfig]]:
        """The data for the artifact model."""
        raise NotImplementedError()

    def __init_subclass__(cls, version: int):
        name = cls.__name__
        if not ALLOW_MODEL_TYPE_OVERWRITES.get() and name in MODEL_TYPES_BY_NAME:
            msg = f"Artifact model named {name!r} already exists"
            raise RuntimeError(msg)
        MODEL_TYPES_BY_NAME[name] = cls
        cls.model_version = version


class ModelMetadata(TypedDict):
    """The metadata for an artifact model."""

    artigraph_version: str
    """The version of Artigraph used to generate the model"""


def _get_field_artifacts_from_models_and_nodes_by_path(
    models_by_path: dict[str, tuple[BaseModel, ModelData]],
    nodes_by_path: dict[str, DatabaseArtifact],
):
    return [
        fn
        for path, (model, data) in models_by_path.items()
        for fn in _model_field_artifacts(model, data, nodes_by_path[path])
    ]


def _get_parent_child_id_pairs_from_nodes_by_path(
    nodes_by_path: dict[str, DatabaseArtifact],
    root_node: Node | None,
) -> list[tuple[int | None, int]]:
    pairs: list[tuple[int | None, int]] = []
    for path, node in nodes_by_path.items():
        parent_node = nodes_by_path.get(_get_model_parent_path(path), root_node)
        parent_node_id = None if parent_node is None else parent_node.node_id
        pairs.append((parent_node_id, node.node_id))
    return pairs


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
    return new_artifact(
        label,
        ModelMetadata(artigraph_version=artigraph.__version__),
        serializer=json_sorted_serializer,
        detail=_get_model_detail(model),
    )[0]


def _get_models_and_data_by_paths(
    model: BaseModel, path: str = ""
) -> dict[str, tuple[BaseModel, ModelData]]:
    """Get the paths of all artifact models in the tree."""

    model_data = model.model_data()
    paths: dict[str, tuple[BaseModel, ModelData]] = {path: (model, model_data)}

    for label, (value, _) in model_data.items():
        maybe_model = try_convert_value_to_modeled_type(value)
        if not isinstance(maybe_model, BaseModel):
            continue
        paths.update(_get_models_and_data_by_paths(maybe_model, f"{path}/{label}").items())

    return paths


def _model_field_artifacts(
    model: BaseModel, model_data: ModelData, parent: DatabaseArtifact
) -> Sequence[QualifiedArtifact]:
    artifacts: list[QualifiedArtifact] = []
    for label, (value, config) in model_data.items():
        if isinstance(value, BaseModel):
            continue

        artifacts.append(
            new_artifact(
                label,
                value,
                serializer=config.get("serializer", json_serializer),
                storage=config.get("storage"),
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
) -> BaseModel:
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

    return cls.model_init(model_version, kwargs)


def _get_model_detail(model: BaseModel) -> str:
    """Get the name and version of the artifact model."""
    return f"{type(model).__name__}-v{model.model_version}"


def _get_model_parent_path(path: str) -> str:
    """Get the path of the parent artifact model."""
    return "__parent_of_root__" if path == "" else path.rsplit("/", 1)[0]


def _get_model_label_from_path(path: str) -> str:
    """Get the name of the artifact model field from a path."""
    return path.rsplit("/", 1)[-1]
