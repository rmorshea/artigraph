from __future__ import annotations

from abc import ABC, abstractclassmethod, abstractmethod
from collections import defaultdict
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any, ClassVar, Iterator, Sequence, TypedDict, TypeVar, cast
from uuid import UUID

from typing_extensions import Self, TypeAlias

from artigraph import __version__ as artigraph_version
from artigraph.core.api.artifact import Artifact, load_deserialized_artifact_value
from artigraph.core.api.filter import NodeFilter, NodeLinkFilter
from artigraph.core.api.funcs import GraphType
from artigraph.core.orm.artifact import (
    OrmArtifact,
    OrmDatabaseArtifact,
    OrmModelArtifact,
    OrmRemoteArtifact,
)
from artigraph.core.orm.base import OrmBase
from artigraph.core.orm.link import OrmNodeLink
from artigraph.core.orm.node import OrmNode
from artigraph.core.serializer.base import Serializer
from artigraph.core.serializer.json import json_serializer, json_sorted_serializer
from artigraph.core.storage.base import Storage

M = TypeVar("M", bound="GraphModel")

ModelData: TypeAlias = "dict[str, tuple[Any, FieldConfig]]"

MODEL_TYPE_BY_NAME: dict[str, type[GraphModel]] = {}
MODELED_TYPES: dict[type[Any], type[GraphModel]] = {}

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


class GraphModel(ABC):
    """A base for all modeled artifacts."""

    graph_orm_type: ClassVar[type[OrmModelArtifact]] = OrmModelArtifact
    """The ORM type for this model."""

    graph_model_name: ClassVar[str]
    """The name of the artifact model."""

    graph_model_version: ClassVar[int] = 1
    """The version of the artifact model."""

    graph_model_id: UUID
    """The unique ID of this model."""

    @abstractmethod
    def graph_model_data(self) -> dict[str, tuple[Any, FieldConfig]]:
        """The data for the artifact model."""
        raise NotImplementedError()

    @abstractclassmethod
    def graph_model_init(cls, version: int, data: dict[str, Any], /) -> Self:
        """Initialize the artifact model, migrating it if necessary."""
        raise NotImplementedError()  # nocov

    def __init_subclass__(cls, version: int, **kwargs: Any):
        cls.graph_model_version = version

        if "graph_model_name" not in cls.__dict__:
            cls.graph_model_name = cls.__name__

        if not ALLOW_MODEL_TYPE_OVERWRITES.get() and cls.graph_model_name in MODEL_TYPE_BY_NAME:
            msg = f"Artifact model {cls.graph_model_name!r} already exists"
            raise RuntimeError(msg)
        else:
            MODEL_TYPE_BY_NAME[cls.graph_model_name] = cls

        super().__init_subclass__(**kwargs)

    def graph_filter_self(self) -> NodeFilter[Any]:
        return NodeFilter(node_id=self.graph_node_id)

    @classmethod
    def graph_filter_related(
        cls, where: NodeFilter[Any]
    ) -> dict[type[OrmNodeLink] | type[OrmNode], NodeLinkFilter]:
        return {
            OrmNode: NodeFilter(descendant_of=where),
            OrmNodeLink: NodeLinkFilter(ancestor=where),
        }

    async def graph_dump_self(self) -> OrmModelArtifact:
        metadata_dict = ModelMetadata(artigraph_version=artigraph_version)
        return self._make_own_metadata_artifact(metadata_dict)

    async def graph_dump_related(self) -> Sequence[OrmBase]:
        orm_objects: list[OrmBase] = []
        for label, (value, config) in self.graph_model_data().items():
            maybe_model = _try_convert_value_to_modeled_type(value)
            if isinstance(maybe_model, GraphModel):
                if config:
                    msg = f"Model artifacts cannot have a config. Got {config}"
                    raise ValueError(msg)
                inner_metadata, *inner_orm_object = await maybe_model.graph_dump()
                if not isinstance(inner_metadata, OrmModelArtifact):
                    msg = f"Expected model artifact, got {inner_metadata}"
                    raise ValueError(msg)
                orm_objects.append(
                    OrmNodeLink(
                        parent_id=self.graph_node_id,
                        child_id=inner_metadata.node_id,
                        label=label,
                    )
                )
                orm_objects.append(inner_metadata)
                orm_objects.extend(inner_orm_object)
            elif isinstance(value, GraphType):
                ...
            else:
                inner_artifact = _make_artifact(value, config)
                orm_objects.append(
                    OrmNodeLink(
                        parent_id=self.graph_node_id,
                        child_id=inner_artifact.node_id,
                        label=label,
                    )
                )
                orm_objects.append(inner_artifact)
        return orm_objects

    @classmethod
    async def graph_load(
        cls,
        records: Sequence[OrmModelArtifact],
        related_records: dict[
            type[OrmNodeLink] | type[OrmNode],
            Sequence[OrmNodeLink] | Sequence[OrmNode],
        ],
    ) -> Sequence[Self]:
        arts_dict_by_p_id = _get_labeled_artifacts_by_parent_id(records, related_records)
        return [
            await _get_model_from_labeled_artifacts_by_parent_id(art, arts_dict_by_p_id)
            for art in records
        ]

    def _make_own_metadata_artifact(self, metadata: ModelMetadata) -> OrmModelArtifact:
        return OrmModelArtifact(
            node_id=self.graph_node_id,
            artifact_serializer=json_sorted_serializer.name,
            database_artifact_data=json_sorted_serializer.serialize(metadata),
            model_artifact_name=self.graph_model_name,
            model_artifact_version=self.graph_model_version,
        )


class ModelMetadata(TypedDict):
    """The metadata for an artifact model."""

    artigraph_version: str
    """The version of Artigraph used to generate the model"""


def _make_artifact(value: Any, config: FieldConfig) -> Artifact[Any]:
    """Make an artifact from a value and config."""
    config = FieldConfig() if config is None else config
    return Artifact(
        value=value,
        serializer=config.get("serializer", json_serializer),
        storage=config.get("storage"),
    )


def _get_labeled_artifacts_by_parent_id(
    records: Sequence[OrmModelArtifact],
    related_records: dict[
        type[OrmNodeLink] | type[OrmArtifact],
        Sequence[OrmNodeLink] | Sequence[OrmArtifact],
    ],
) -> dict[UUID, dict[str, OrmArtifact]]:
    links = cast(Sequence[OrmNodeLink], related_records[OrmNodeLink])
    artifacts = cast(Sequence[OrmArtifact], [*related_records[OrmArtifact], *records])

    artifacts_by_id = {art.node_id: art for art in artifacts}
    artifacts_by_parent_id: defaultdict[UUID, dict[str, OrmArtifact]] = defaultdict(dict)
    for link in links:
        artifacts_by_parent_id[link.parent_id][link.label] = artifacts_by_id[link.child_id]

    return artifacts_by_parent_id


def _get_model_from_labeled_artifacts_by_parent_id(
    model_metadata_artifact: OrmModelArtifact,
    labeled_artifacts_by_parent_id: dict[UUID, dict[str, OrmDatabaseArtifact | OrmRemoteArtifact]],
) -> GraphModel:
    labeled_children = labeled_artifacts_by_parent_id[model_metadata_artifact.node_id]

    kwargs: dict[str, Any] = {}
    for label, child in labeled_children.items():
        if isinstance(child, OrmModelArtifact):
            value = _get_model_from_labeled_artifacts_by_parent_id(
                child,
                labeled_artifacts_by_parent_id,
            )
        else:
            value = load_deserialized_artifact_value(child)
        kwargs[label] = value

    cls = MODEL_TYPE_BY_NAME[model_metadata_artifact.model_artifact_name]
    return cls.graph_model_init(model_metadata_artifact.model_artifact_version, kwargs)


def _try_convert_value_to_modeled_type(value: Any) -> GraphModel | Any:
    """Try to convert a value to a modeled type."""
    modeled_type = MODELED_TYPES.get(type(value))
    if modeled_type is not None:
        return modeled_type(value)  # type: ignore
    return value
