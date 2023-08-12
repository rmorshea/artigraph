from __future__ import annotations

from contextvars import ContextVar
from typing import Any, ClassVar, Sequence, TypedDict, TypeVar

from typing_extensions import Self, TypeAlias

import artigraph
from artigraph.api.artifact import (
    AnyQualifiedArtifact,
    QualifiedArtifact,
    group_artifacts_by_parent_id,
    new_artifact,
    read_artifact,
    read_artifact_or_none,
    read_artifacts,
    write_artifacts,
)
from artigraph.api.filter import ArtifactFilter
from artigraph.api.node import read_node, write_parent_child_relationships
from artigraph.db import session_context
from artigraph.model.filter import ModelFilter
from artigraph.orm import DatabaseArtifact, Node
from artigraph.orm.artifact import ModelArtifact
from artigraph.serializer import Serializer
from artigraph.serializer.json import json_serializer, json_sorted_serializer
from artigraph.storage import Storage

ModelData: TypeAlias = "dict[str, tuple[Any, FieldConfig]]"
M = TypeVar("M", bound="BaseModel")

MODEL_TYPES_BY_NAME: dict[str, type[BaseModel]] = {}
MODELED_TYPES: dict[type[Any], type[BaseModel]] = {}

# useful in an interactive context (e.g. IPython/Jupyter)
ALLOW_MODEL_TYPE_OVERWRITES = ContextVar("ALLOW_MODEL_TYPE_OVERWRITES", default=False)

QualifiedModelArtifact: TypeAlias = "QualifiedArtifact[ModelArtifact, ModelMetadata]"
"""A convenience type for a qualified model artifact."""


async def write_models(*, parent_id: int | None, models: dict[str, BaseModel]) -> dict[str, int]:
    """Save a set of models in the database that are linked to the given node"""
    ids: dict[str, int] = {}
    for k, v in models.items():
        ids[k] = await write_model(parent_id=parent_id, label=k, mode=v)
    return ids


async def write_model(*, parent_id: int | None, label: str, model: BaseModel) -> int:
    """Save a model in te database that is linked to the given node"""
    parent_node = None if parent_id is None else await read_node(parent_id)

    models_and_data_by_path = _get_models_and_data_by_paths(model)
    nodes_by_path = {
        path: _get_model_artifact(_get_model_label_from_path(path), model)
        for path, (model, _) in models_and_data_by_path.items()
    }

    root_node = nodes_by_path[""]
    root_node.artifact.node_parent_id = parent_id
    root_node.artifact.artifact_label = label

    async with session_context(expire_on_commit=False):
        # write the model nodes
        await write_artifacts(list(nodes_by_path.values()))

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


async def read_model(model_filter: ModelFilter[M]) -> M:
    """Load the model from the database."""
    return await _read_model(await read_artifact(model_filter))


async def read_model_or_none(model_filter: ModelFilter[M]) -> M | None:
    """Load the model from the database or return None if it doesn't exist."""
    qual = await read_artifact_or_none(model_filter)
    if qual is None:
        return None
    return await _read_model(qual)


async def read_models(model_filter: ModelFilter[M]) -> Sequence[M]:
    """Load the models from the database."""
    return [await _read_model(a) for a in await read_artifacts(model_filter)]


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


async def _read_model(qual: QualifiedModelArtifact) -> BaseModel:
    """Load the artifact model from the database."""
    desc_artifacts = await read_artifacts(ArtifactFilter(is_descendant_of=[qual.artifact.node_id]))
    desc_artifacts_by_parent_id = group_artifacts_by_parent_id(desc_artifacts)
    return await _load_from_artifacts(qual.artifact, desc_artifacts_by_parent_id)


def _get_field_artifacts_from_models_and_nodes_by_path(
    models_by_path: dict[str, tuple[BaseModel, ModelData]],
    nodes_by_path: dict[str, DatabaseArtifact],
):
    return [
        fn
        for path, (_, data) in models_by_path.items()
        for fn in _artifacts_from_model_data(data, nodes_by_path[path])
    ]


def _get_parent_child_id_pairs_from_nodes_by_path(
    nodes_by_path: dict[str, QualifiedModelArtifact],
    root_node: Node | None,
) -> list[tuple[int | None, int]]:
    pairs: list[tuple[int | None, int]] = []
    for path, qual in nodes_by_path.items():
        parent_qual = nodes_by_path.get(_get_model_parent_path(path), root_node)
        parent_node_id = None if parent_qual is None else parent_qual.artifact.node_id
        pairs.append((parent_node_id, qual.artifact.node_id))
    return pairs


def _get_model_artifact(label: str, model: BaseModel) -> QualifiedModelArtifact:
    """Get the artifact model for the given artifact label."""
    return QualifiedArtifact(
        artifact=ModelArtifact(
            node_parent_id=None,
            artifact_label=label,
            artifact_serializer=json_sorted_serializer.name,
            model_artifact_type=type(model).__name__,
            model_artifact_version=model.model_version,
        ),
        value=ModelMetadata(artigraph_version=artigraph.__version__),
    )


def _get_models_and_data_by_paths(
    model: BaseModel, path: str = ""
) -> dict[str, tuple[BaseModel, ModelData]]:
    """Get the paths of all artifact models in the tree."""

    model_data = model.model_data()
    paths: dict[str, tuple[BaseModel, ModelData]] = {path: (model, model_data)}

    for label, (value, _) in model_data.items():
        maybe_model = _try_convert_value_to_modeled_type(value)
        if not isinstance(maybe_model, BaseModel):
            continue
        paths.update(_get_models_and_data_by_paths(maybe_model, f"{path}/{label}").items())

    return paths


def _artifacts_from_model_data(
    model_data: ModelData, parent: DatabaseArtifact
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
            )
        )
    return artifacts


async def _load_from_artifacts(
    qual_artifact: QualifiedModelArtifact,
    artifacts_by_parent_id: dict[int | None, list[AnyQualifiedArtifact]],
) -> BaseModel:
    """Load the artifacts from the database."""
    cls = _get_model_type_by_name(qual_artifact.artifact.model_artifact_type)
    version = qual_artifact.artifact.model_artifact_version
    kwargs: dict[str, Any] = {}

    for child_qual_artifact in artifacts_by_parent_id[qual_artifact.artifact.node_id]:
        if isinstance(child_qual_artifact.artifact, ModelArtifact):
            kwargs[child_qual_artifact.artifact.artifact_label] = await _load_from_artifacts(
                child_qual_artifact,
                artifacts_by_parent_id,
            )
        else:
            kwargs[child_qual_artifact.artifact.artifact_label] = child_qual_artifact.value

    return cls.model_init(version, kwargs)


def _get_model_parent_path(path: str) -> str:
    """Get the path of the parent artifact model."""
    return "__parent_of_root__" if path == "" else path.rsplit("/", 1)[0]


def _get_model_label_from_path(path: str) -> str:
    """Get the name of the artifact model field from a path."""
    return path.rsplit("/", 1)[-1]


def _try_convert_value_to_modeled_type(value: Any) -> BaseModel | Any:
    """Try to convert a value to a modeled type."""
    modeled_type = MODELED_TYPES.get(type(value))
    if modeled_type is not None:
        return modeled_type(value)  # type: ignore
    return value


def _get_model_type_by_name(name: str) -> type[BaseModel]:
    """Get an artifact model type by its name."""
    try:
        return MODEL_TYPES_BY_NAME[name]
    except KeyError:
        msg = f"Unknown artifact model type {name!r}"
        raise ValueError(msg) from None
