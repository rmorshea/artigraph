from __future__ import annotations

from abc import ABC
from collections import defaultdict
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import field
from logging import Filter
from typing import Any, ClassVar, Iterator, Sequence, TypedDict, TypeVar
from uuid import UUID

from typing_extensions import Self, TypeAlias

from artigraph import __version__ as artigraph_version
from artigraph.api.artifact import Artifact
from artigraph.api.filter import NodeFilter, NodeLinkFilter
from artigraph.api.funcs import orm_read, read
from artigraph.api.link import NodeLink
from artigraph.db import new_session
from artigraph.orm.artifact import OrmModelArtifact
from artigraph.orm.base import OrmBase
from artigraph.orm.link import OrmNodeLink
from artigraph.orm.node import OrmNode
from artigraph.serializer import Serializer
from artigraph.serializer.json import JsonSerializer, json_serializer, json_sorted_serializer
from artigraph.storage import Storage
from artigraph.utils import TaskBatch

M = TypeVar("M", bound="BaseModel")

ModelData: TypeAlias = "dict[str, tuple[Any, FieldConfig]]"

MODEL_TYPE_BY_NAME: dict[str, type[BaseModel]] = {}
MODELED_TYPES: dict[type[Any], type[BaseModel]] = {}

# useful in an interactive context (e.g. IPython/Jupyter)
ALLOW_MODEL_TYPE_OVERWRITES = ContextVar("ALLOW_MODEL_TYPE_OVERWRITES", default=False)


@contextmanager
def allow_model_type_overwrites() -> Iterator[None]:
    """A context in which it's possible to overwrite already defined model types"""
    reset_token = ALLOW_MODEL_TYPE_OVERWRITES.set(True)
    try:
        yield
    finally:
        ALLOW_MODEL_TYPE_OVERWRITES.reset(reset_token)


class FieldConfig(TypedDict, total=False):
    """The metadata for an artifact model field."""

    serializer: Serializer
    """The serializer for the artifact model field."""

    storage: Storage
    """The storage for the artifact model field."""

    annotation: Any
    """The type annotation for the artifact model field."""


class BaseModel(ABC):
    """An interface for all modeled artifacts."""

    orm_type: ClassVar[type[OrmModelArtifact]] = OrmModelArtifact
    """The ORM type for this model."""

    model_name: ClassVar[str]
    """The name of the artifact model."""

    model_version: ClassVar[int] = 1
    """The version of the artifact model."""

    def __init_subclass__(cls, version: int):
        cls.model_version = version
        if "model_name" not in cls.__dict__:
            cls.model_name = cls.__name__

        if not ALLOW_MODEL_TYPE_OVERWRITES.get() and cls.model_name in MODEL_TYPE_BY_NAME:
            msg = f"Artifact model {cls.model_name!r} already exists"
            raise RuntimeError(msg)

        MODEL_TYPE_BY_NAME[cls.model_name] = cls

    node_id: UUID
    """The unique ID of this model."""

    @classmethod
    def model_init(cls, version: int, data: dict[str, Any], /) -> Self:
        """Initialize the artifact model, migrating it if necessary."""
        raise NotImplementedError()  # nocov

    def model_data(self) -> dict[str, tuple[Any, FieldConfig]]:
        """The data for the artifact model."""
        raise NotImplementedError()

    def orm_filter_self(self) -> NodeFilter[Any]:
        return NodeFilter(node_id=self.node_id)

    @classmethod
    def orm_filter_related(
        cls, where: NodeFilter[Any]
    ) -> dict[type[OrmNodeLink] | type[OrmNode], NodeLinkFilter]:
        return {
            OrmNode: NodeFilter(descendant_of=where),
            OrmNodeLink: NodeLinkFilter(ancestor=where),
        }

    async def orm_dump(self) -> Sequence[OrmBase]:
        metadata_dict = ModelMetadata(artigraph_version=artigraph_version)

        orm_objects: list[OrmBase] = [self._make_own_metadata_artifact(metadata_dict)]

        for label, (value, config) in self.model_data().items():
            maybe_model = _try_convert_value_to_modeled_type(value)
            if isinstance(maybe_model, BaseModel):
                if config:
                    msg = f"Model artifacts cannot have a config. Got {config}"
                    raise ValueError(msg)
                inner_metadata, *inner_orm_object = await maybe_model.orm_dump()
                if not isinstance(inner_metadata, OrmModelArtifact):
                    msg = f"Expected model artifact, got {inner_metadata}"
                    raise ValueError(msg)
                orm_objects.append(
                    OrmNodeLink(
                        parent_id=self.node_id,
                        child_id=inner_metadata.node_id,
                        label=label,
                    )
                )
                orm_objects.append(inner_metadata)
                orm_objects.extend(inner_orm_object)
            else:
                inner_artifact = _make_artifact(value, config)
                orm_objects.append(
                    OrmNodeLink(
                        parent_id=self.node_id,
                        child_id=inner_artifact.node_id,
                        label=label,
                    )
                )
                orm_objects.append(inner_artifact)

    @classmethod
    async def orm_load(
        cls,
        records: Sequence[OrmModelArtifact],
        related_records: dict[
            type[OrmNodeLink] | type[OrmNode],
            Sequence[OrmNodeLink] | Sequence[OrmNode],
        ],
    ) -> Sequence[Self]:
        ...

    def _make_own_metadata_artifact(self, metadata: ModelMetadata) -> OrmModelArtifact:
        return OrmModelArtifact(
            node_id=self.node_id,
            artifact_serializer=json_sorted_serializer.name,
            database_artifact_data=json_sorted_serializer.serialize(metadata),
            model_artifact_name=self.model_name,
            model_artifact_version=self.model_version,
        )


class ModelMetadata(TypedDict):
    """The metadata for an artifact model."""

    artigraph_version: str
    """The version of Artigraph used to generate the model"""


class ModelArtifact(Artifact[OrmModelArtifact, M]):
    """An artifact storing metadata about a model."""

    orm_type: ClassVar[type[OrmModelArtifact]] = OrmModelArtifact

    value: M
    serializer: None = field(init=False, default=None)
    storage: None = field(init=False, default=None)

    @property
    def version(self) -> int:
        return self.value.model_version

    def filters(self) -> dict[type[OrmBase], Filter]:
        return {
            OrmNode: (
                NodeFilter(node_id=self.node_id)
                # select all descendants of this node
                | NodeFilter(descendant_of=self.node_id)
            ),
            OrmNodeLink: (
                # select direct links to/from this node
                NodeLinkFilter(parent=self.node_id, child=self.node_id),
                # select all ancestors of this node
                NodeLinkFilter(ancestor=self.node_id),
            ),
        }

    async def to_orms(self) -> Sequence[OrmBase]:
        root_art = _make_model_artifact(self.value, FieldConfig(), self.node_id)

        orms: TaskBatch[Sequence[OrmBase]] = TaskBatch()
        orms.add(root_art.to_orms)
        for parent_id, artifacts in _get_model_artifacts_by_parent_id(root_art, self.value).items():
            for label, art in artifacts.items():
                orms.add(art.to_orms)
                if label is not None:
                    link = NodeLink(parent_id=parent_id, child_id=art.node_id, label=label)
                    orms.add(link.to_orms)

        return [o for os in await orms.gather() for o in os]

    @classmethod
    async def from_orm(cls, root_orm: OrmModelArtifact) -> Self:
        root_art = await ModelMetadataArtifact.from_orm(root_orm)

        async with new_session():
            node_links = await read(NodeLink, NodeLinkFilter(ancestor=root_orm.node_id))
            descendant_nodes = await orm_read(OrmNode, NodeFilter(descendant_of=root_orm.node_id))

        artifacts: TaskBatch[Artifact[Any]] = TaskBatch()
        for node in descendant_nodes:
            if isinstance(node, OrmModelArtifact):
                artifacts.add(ModelMetadataArtifact.from_orm, node)
            else:
                artifacts.add(Artifact.from_orm, node)

        artifacts_by_id = {art.node_id: art for art in await artifacts.gather()}

        artifacts_by_parent_id: dict[str | None, dict[str, Artifact[Any]]] = defaultdict(dict)
        for link in node_links:
            artifacts_by_parent_id[link.parent_id][link.label] = artifacts_by_id[link.child_id]

        value = _model_from_artifacts_by_parent_id(root_art, artifacts_by_parent_id)

        return cls(
            value=value,
            node_id=root_art.node_id,
            orm=root_orm,
        )


class ModelMetadataArtifact(Artifact[OrmModelArtifact, ModelMetadata]):
    """Stored metadata about a model."""

    value: ModelMetadata
    model_name: str
    model_version: int
    serializer: JsonSerializer = field(init=False, default=json_sorted_serializer)
    storage: None = field(init=False, default=None)

    async def to_orms(self) -> Sequence[OrmBase]:
        return [
            OrmModelArtifact(
                node_id=self.node_id,
                artifact_serializer=self.serializer.name,
                database_artifact_data=self.serializer.serialize(self.value),
                model_artifact_name=self.model_name,
                model_artifact_version=self.model_version,
            )
        ]

    @classmethod
    async def from_orm(cls, orm: OrmModelArtifact) -> Self:
        return cls(
            value=ModelMetadata(artigraph_version=orm.model_artifact_name),
            node_id=orm.node_id,
            model_name=orm.model_artifact_name,
            model_version=orm.model_artifact_version,
            orm=orm,
        )


def _get_model_artifacts_by_parent_id(
    model_artifact: ModelMetadataArtifact, model: BaseModel
) -> dict[str | None, dict[str | None, Artifact[Any]]]:
    """Get labeled model artifacts grouped by their parent"""
    arts_by_parent_id: defaultdict[str | None, dict[str, Artifact[Any]]] = defaultdict(dict)
    for label, (value, config) in model.model_data().items():
        maybe_model = _try_convert_value_to_modeled_type(value)
        if isinstance(maybe_model, BaseModel):
            child_model_art = _make_model_artifact(maybe_model, config, model_artifact.node_id)
            arts_by_parent_id[model_artifact.node_id][label] = child_model_art
            for p_id, c_art in _get_model_artifacts_by_parent_id(
                child_model_art, model=maybe_model
            ).items():
                arts_by_parent_id[p_id].update(c_art)
        else:
            arts_by_parent_id[model_artifact.node_id][label] = _make_artifact(maybe_model, config)
    return arts_by_parent_id


def _make_artifact(value: Any, config: FieldConfig) -> Artifact[Any]:
    """Make an artifact from a value and config."""
    config = FieldConfig() if config is None else config
    return Artifact(
        value=value,
        serializer=config.get("serializer", json_serializer),
        storage=config.get("storage"),
    )


def _make_model_artifact(
    value: BaseModel,
    config: FieldConfig,
    node_id: str | None,
) -> Artifact[Any]:
    """Make an artifact from a model and config."""
    if config:
        msg = f"Model artifacts cannot have a config. Got {config}"
        raise ValueError(msg)
    return ModelMetadataArtifact(
        value=ModelMetadata(artigraph_version=artigraph.__version__),
        model_name=value.model_name,
        model_version=value.model_version,
        node_id=node_id or make_uuid(),
    )


def _model_from_artifacts_by_parent_id(
    model_metadata_artifact: ModelMetadataArtifact,
    artifacts_by_parent_id: dict[str, dict[str, Artifact]],
) -> BaseModel:
    children = artifacts_by_parent_id[model_metadata_artifact.node_id]

    kwargs: dict[str, Any] = {}
    for label, child in children.items():
        if isinstance(child, ModelMetadataArtifact):
            value = _model_from_artifacts_by_parent_id(child, artifacts_by_parent_id)
        else:
            value = child.value
        kwargs[label] = value

    cls = MODEL_TYPE_BY_NAME[model_metadata_artifact.model_name]
    return cls.model_init(model_metadata_artifact.model_version, kwargs)


def _try_convert_value_to_modeled_type(value: Any) -> BaseModel | Any:
    """Try to convert a value to a modeled type."""
    modeled_type = MODELED_TYPES.get(type(value))
    if modeled_type is not None:
        return modeled_type(value)  # type: ignore
    return value
