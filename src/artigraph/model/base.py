from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any, ClassVar, Iterator, Sequence, TypedDict, TypeVar

from typing_extensions import Self, TypeAlias, TypeGuard

import artigraph
from artigraph.api.artifact import (
    AnyQualifiedArtifact,
    QualifiedArtifact,
    delete_artifacts,
    read_artifact,
    read_artifact_or_none,
    read_artifacts,
    write_artifacts,
)
from artigraph.api.filter import ArtifactFilter, Filter, NodeLinkFilter
from artigraph.api.node import read_node_links, write_node_links
from artigraph.db import new_session
from artigraph.model.filter import ModelFilter
from artigraph.orm.artifact import DatabaseArtifact, ModelArtifact, RemoteArtifact
from artigraph.orm.link import NodeLink
from artigraph.serializer import Serializer
from artigraph.serializer.json import json_serializer
from artigraph.storage import Storage

ModelData: TypeAlias = "dict[str, tuple[Any, FieldConfig]]"
M = TypeVar("M", bound="BaseModel")

MODEL_TYPE_BY_NAME: dict[str, type[BaseModel]] = {}
MODELED_TYPES: dict[type[Any], type[BaseModel]] = {}

# useful in an interactive context (e.g. IPython/Jupyter)
ALLOW_MODEL_TYPE_OVERWRITES = ContextVar("ALLOW_MODEL_TYPE_OVERWRITES", default=False)

QualifiedModelMetadataArtifact: TypeAlias = "QualifiedArtifact[ModelArtifact, ModelMetadata]"
"""A convenience type for a qualified model metadata artifact."""

QualifiedModelArtifact: TypeAlias = "QualifiedArtifact[ModelArtifact, M]"
"""A convenience type for a qualified model artifact."""


@contextmanager
def allow_model_type_overwrites() -> Iterator[None]:
    """A context in which it's possible to overwrite already defined model types"""
    reset_token = ALLOW_MODEL_TYPE_OVERWRITES.set(True)
    try:
        yield
    finally:
        ALLOW_MODEL_TYPE_OVERWRITES.reset(reset_token)


async def write_model(model: M) -> QualifiedModelArtifact[M]:
    """Save a model in the database"""
    return (await write_models([model]))[0]


async def write_models(models: Sequence[M]) -> Sequence[QualifiedModelMetadataArtifact[M]]:
    """Save the models in the database."""
    qual_artifacts: list[QualifiedArtifact[Any]] = []
    node_links: list[NodeLink] = []
    results: list[QualifiedModelArtifact[M]] = []
    for model in models:
        quals, links = _get_model_artifacts_and_links(model)
        qual_artifacts.extend(quals)
        node_links.extend(links)
        results.append(quals[0])
    async with new_session(expire_on_commit=False):
        await write_artifacts(qual_artifacts)
        await write_node_links(node_links)
    return results


async def read_model(model_filter: ModelFilter[M] | Filter) -> QualifiedModelArtifact[M]:
    """Load the model from the database."""
    qual = await read_artifact(model_filter)
    return QualifiedArtifact(qual.artifact, await _read_model(qual))  # type: ignore


async def read_model_or_none(
    model_filter: ModelFilter[M] | Filter,
) -> QualifiedModelArtifact[M] | None:
    """Load the model from the database or return None if it doesn't exist."""
    qual = await read_artifact_or_none(model_filter)
    if qual is None:
        return None
    return QualifiedArtifact(qual.artifact, await _read_model(qual))  # type: ignore


async def read_models(model_filter: ModelFilter[M] | Filter) -> Sequence[QualifiedModelArtifact[M]]:
    """Load the models from the database."""
    root_model_artifacts = await read_artifacts(model_filter)
    node_ids = [q.artifact.node_id for q in root_model_artifacts]

    node_link_filter = NodeLinkFilter(descendant_of=node_ids)
    artifact_filter = ArtifactFilter(child=node_link_filter)

    all_artifacts = await read_artifacts(artifact_filter)
    node_links = await read_node_links(node_link_filter)

    raise NotImplementedError()


async def delete_models(model_filter: ModelFilter[Any] | Filter) -> None:
    """Delete the models from the database."""
    model_ids = [q.artifact.node_id for q in await read_artifacts(model_filter)]
    await delete_artifacts(NodeLinkFilter(descendant_of=model_ids, include_self=True))


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

    model_name: ClassVar[str]
    """The name of the artifact model."""

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
        cls.model_version = version
        if "model_name" not in cls.__dict__:
            cls.model_name = cls.__name__

        if not ALLOW_MODEL_TYPE_OVERWRITES.get() and cls.model_name in MODEL_TYPE_BY_NAME:
            msg = f"Artifact model {cls.model_name!r} already exists"
            raise RuntimeError(msg)

        MODEL_TYPE_BY_NAME[cls.model_name] = cls


class ModelMetadata(TypedDict):
    """The metadata for an artifact model."""

    artigraph_version: str
    """The version of Artigraph used to generate the model"""


async def _read_model(qual: QualifiedModelMetadataArtifact) -> BaseModel:
    """Load the artifact model from the database."""
    desc_artifacts = await read_artifacts(
        ArtifactFilter(
            child=NodeLinkFilter(
                descendant_of=[qual.artifact.node_id],
                include_self=True,
            )
        )
    )
    desc_artifacts_by_parent_id = await group_artifacts_by_parent_id(desc_artifacts)
    return await _load_from_artifacts(qual, desc_artifacts_by_parent_id)


def _get_model_artifacts_and_links(
    model: BaseModel,
    label: str | None = None,
    parent_id: int | None = None,
) -> tuple[Sequence[QualifiedArtifact], Sequence[NodeLink]]:
    """Get the paths of all artifact models in the tree."""
    model_artifact = ModelArtifact(
        model_artifact_type=model.model_name,
        model_artifact_version=model.model_version,
    )
    qual_artifacts: list[QualifiedArtifact] = [
        QualifiedArtifact(
            artifact=model_artifact,
            value=ModelMetadata(artigraph_version=artigraph.__version__),
        )
    ]
    node_links: list[NodeLink] = []
    if parent_id is not None or label is not None:
        node_links.append(
            NodeLink(parent_id=parent_id, child_id=model_artifact.node_id, label=label)
        )

    for key, (value, config) in model.model_data().items():
        if isinstance(value, BaseModel):
            child_artifacts, child_links = _get_model_artifacts_and_links(
                value,
                label=key,
                parent_id=model_artifact.node_id,
            )
            qual_artifacts.extend(child_artifacts)
            node_links.extend(child_links)
        else:
            data_artifact, data_link = _make_model_data_artifact_and_link(
                key, value, config, model_artifact.node_id
            )
            qual_artifacts.append(data_artifact)
            node_links.append(data_link)

    return qual_artifacts, node_links


def _make_model_data_artifact_and_link(
    key: str, value: Any, config: FieldConfig, parent_id: int
) -> tuple[QualifiedArtifact[Any], NodeLink]:
    storage = config.get("storage")
    serializer = config.get("serializer", json_serializer)
    data_artifact = (
        DatabaseArtifact(
            artifact_serializer=serializer.name,
        )
        if storage is None
        else RemoteArtifact(
            artifact_serializer=serializer.name,
            remote_artifact_storage=storage.name,
        )
    )

    qual = QualifiedArtifact(
        artifact=data_artifact,
        value=value,
    )
    link = NodeLink(
        parent_id=parent_id,
        child_id=data_artifact.node_id,
        label=key,
    )

    return qual, link


async def _load_from_artifacts(
    qual_artifact: QualifiedModelMetadataArtifact,
    artifacts_by_parent_id: dict[int | None, list[AnyQualifiedArtifact]],
) -> BaseModel:
    """Load the artifacts from the database."""
    cls = _get_model_type_by_name(qual_artifact.artifact.model_artifact_type)
    version = qual_artifact.artifact.model_artifact_version
    kwargs: dict[str, Any] = {}

    for child_qual_artifact in artifacts_by_parent_id.get(qual_artifact.artifact.node_id, []):
        if _is_qualified_model_artifact(child_qual_artifact):
            kwargs[child_qual_artifact.artifact.artifact_label] = await _load_from_artifacts(
                child_qual_artifact,
                artifacts_by_parent_id,
            )
        else:
            kwargs[child_qual_artifact.artifact.artifact_label] = child_qual_artifact.value

    return cls.model_init(version, kwargs)


def _get_model_type_by_name(name: str) -> type[BaseModel]:
    """Get an artifact model type by its name."""
    try:
        return MODEL_TYPE_BY_NAME[name]
    except KeyError:  # nocov
        msg = f"Unknown artifact model type {name!r}"
        raise ValueError(msg) from None


def _is_qualified_model_artifact(
    qual: QualifiedArtifact,
) -> TypeGuard[QualifiedModelMetadataArtifact]:
    """Check if an artifact is a qualified model artifact."""
    return isinstance(qual.artifact, ModelArtifact)
