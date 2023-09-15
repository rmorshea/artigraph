from __future__ import annotations

from collections import defaultdict
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any, ClassVar, Iterator, Mapping, Sequence, TypedDict, TypeVar, cast
from uuid import UUID

from attr import dataclass
from typing_extensions import Self, TypeAlias

from artigraph import __version__ as artigraph_version
from artigraph.core.api.artifact import Artifact, load_deserialized_artifact_value
from artigraph.core.api.filter import Filter, NodeFilter, NodeLinkFilter
from artigraph.core.api.funcs import GraphBase, dump_one, dump_one_flat
from artigraph.core.api.link import NodeLink
from artigraph.core.orm.artifact import (
    OrmArtifact,
    OrmModelArtifact,
)
from artigraph.core.orm.base import OrmBase
from artigraph.core.orm.link import OrmNodeLink
from artigraph.core.orm.node import OrmNode
from artigraph.core.serializer.base import Serializer
from artigraph.core.serializer.json import json_serializer, json_sorted_serializer
from artigraph.core.storage.base import Storage
from artigraph.core.utils.misc import TaskBatch

M = TypeVar("M", bound="GraphModel")

ModelData: TypeAlias = "dict[str, tuple[Any, FieldConfig]]"
LabeledArtifactsByParentId: TypeAlias = Mapping[UUID, dict[str, OrmArtifact]]

MODEL_TYPE_BY_NAME: dict[str, type[GraphModel]] = {}
MODELED_TYPES: dict[type[Any], type[GraphModel]] = {}

# useful in an interactive context (e.g. IPython/Jupyter)
ALLOW_MODEL_TYPE_OVERWRITES = ContextVar("ALLOW_MODEL_TYPE_OVERWRITES", default=False)


def get_model_type_by_name(name: str) -> type[GraphModel]:
    """Get a model type by name."""
    try:
        return MODEL_TYPE_BY_NAME[name]
    except KeyError:  # nocov
        msg = f"Model type {name!r} does not exist"
        raise ValueError(msg) from None


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

    serializers: Sequence[Serializer]
    """The serializer for the artifact model field."""

    storage: Storage
    """The storage for the artifact model field."""


class GraphModel(GraphBase[OrmModelArtifact, OrmBase, NodeFilter[Any]]):
    """A base for all modeled artifacts."""

    graph_orm_type: ClassVar[type[OrmModelArtifact]] = OrmModelArtifact
    """The ORM type for this model."""

    graph_model_name: ClassVar[str]
    """The name of the artifact model."""

    graph_model_version: ClassVar[int] = 1
    """The version of the artifact model."""

    graph_node_id: UUID
    """The unique ID of this model."""

    def graph_model_data(self) -> dict[str, tuple[Any, FieldConfig]]:
        """The data for the artifact model."""
        raise NotImplementedError()

    @classmethod
    def graph_model_init(
        cls,
        info: ModelInfo,  # noqa: ARG003
        kwargs: dict[str, Any],
        /,
    ) -> Self:  # nocov
        """Initialize the artifact model, migrating it if necessary."""
        return cls(**kwargs)

    def __init_subclass__(cls, version: int, **kwargs: Any):
        cls.graph_model_version = version

        if "graph_model_name" not in cls.__dict__:
            cls.graph_model_name = cls.__name__

        if not ALLOW_MODEL_TYPE_OVERWRITES.get() and cls.graph_model_name in MODEL_TYPE_BY_NAME:
            msg = (
                f"Artifact model {cls.graph_model_name!r} already exists "
                f"as {MODEL_TYPE_BY_NAME[cls.graph_model_name]}"
            )
            raise RuntimeError(msg)
        else:
            MODEL_TYPE_BY_NAME[cls.graph_model_name] = cls

        super().__init_subclass__(**kwargs)

    def graph_filter_self(self) -> NodeFilter[Any]:
        return NodeFilter(node_id=self.graph_node_id)

    @classmethod
    def graph_filter_related(cls, where: NodeFilter[Any]) -> dict[type[OrmBase], Filter]:
        return {
            OrmArtifact: NodeFilter(descendant_of=where),
            OrmNodeLink: NodeLinkFilter(ancestor=where),
        }

    async def graph_dump_self(self) -> OrmModelArtifact:
        metadata_dict = ModelMetadata(artigraph_version=artigraph_version)
        return self._graph_make_own_metadata_artifact(metadata_dict)

    async def graph_dump_related(self) -> Sequence[OrmBase]:
        dump_related: TaskBatch[Sequence[OrmBase]] = TaskBatch()
        for label, (value, config) in self.graph_model_data().items():
            maybe_model = _try_convert_value_to_modeled_type(value)
            if not any(v for v in config.values()) and isinstance(maybe_model, GraphBase):
                dump_related.add(_dump_and_link, maybe_model, self.graph_node_id, label)
            else:
                art = _make_artifact(value, config)
                dump_related.add(_dump_and_link, art, self.graph_node_id, label)
        return [r for rs in await dump_related.gather() for r in rs]

    @classmethod
    async def graph_load(
        cls,
        self_records: Sequence[OrmModelArtifact],
        related_records: dict[type[OrmBase], Sequence[OrmBase]],
    ) -> Sequence[Self]:
        arts_dict_by_p_id = _get_labeled_artifacts_by_parent_id(self_records, related_records)
        return [
            (
                await model_type._graph_load_from_labeled_artifacts_by_parent_id(
                    art, arts_dict_by_p_id
                )
            )
            for art in self_records
            if issubclass(model_type := get_model_type_by_name(art.model_artifact_type_name), cls)
        ]

    @classmethod
    async def _graph_load_from_labeled_artifacts_by_parent_id(
        cls,
        model_metadata_artifact: OrmModelArtifact,
        labeled_artifacts_by_parent_id: LabeledArtifactsByParentId,
    ) -> Self:
        return cls.graph_model_init(
            ModelInfo(
                node_id=model_metadata_artifact.node_id,
                version=model_metadata_artifact.model_artifact_version,
                metadata=await load_deserialized_artifact_value(model_metadata_artifact),
            ),
            await cls._graph_load_kwargs_from_labeled_artifacts_by_parent_id(
                model_metadata_artifact,
                labeled_artifacts_by_parent_id,
            ),
        )

    @classmethod
    async def _graph_load_kwargs_from_labeled_artifacts_by_parent_id(
        cls,
        model_metadata_artifact: OrmModelArtifact,
        labeled_artifacts_by_parent_id: LabeledArtifactsByParentId,
    ) -> dict[str, Any]:
        labeled_children = labeled_artifacts_by_parent_id[model_metadata_artifact.node_id]

        load_field_values: TaskBatch[Any] = TaskBatch()
        for child in labeled_children.values():
            if isinstance(child, OrmModelArtifact):
                child_cls = get_model_type_by_name(child.model_artifact_type_name)
                load_field_values.add(
                    child_cls._graph_load_from_labeled_artifacts_by_parent_id,
                    child,
                    labeled_artifacts_by_parent_id,
                )
            else:
                load_field_values.add(
                    load_deserialized_artifact_value,
                    child,
                )

        return dict(zip(labeled_children.keys(), await load_field_values.gather()))

    def _graph_make_own_metadata_artifact(self, metadata: ModelMetadata) -> OrmModelArtifact:
        return OrmModelArtifact(
            node_id=self.graph_node_id,
            artifact_serializer=json_sorted_serializer.name,
            database_artifact_data=json_sorted_serializer.serialize(metadata),
            model_artifact_type_name=self.graph_model_name,
            model_artifact_version=self.graph_model_version,
        )


@dataclass(frozen=True)
class ModelInfo:
    """The info for an artifact model."""

    node_id: UUID
    """The unique ID of the artifact model."""
    version: int
    """The version of the artifact model."""
    metadata: ModelMetadata
    """The metadata for the artifact model"""


class ModelMetadata(TypedDict):
    """The metadata for an artifact model."""

    artigraph_version: str
    """The version of Artigraph used to generate the model"""


def _make_artifact(value: Any, config: FieldConfig) -> Artifact[Any]:
    """Make an artifact from a value and config."""
    config = FieldConfig() if config is None else config
    return Artifact(
        value=value,
        serializer=_pick_serializer(value, config.get("serializers", [])) or json_serializer,
        storage=config.get("storage"),
    )


def _pick_serializer(value: Any, serializers: Sequence[Serializer]) -> Serializer | None:
    if not serializers:
        return None

    if len(serializers) == 1:
        return serializers[0]

    for s in serializers:
        if s.serializable(value):
            return s

    msg = f"Could not find a serializer for {value} among {serializers}"  # nocov
    raise ValueError(msg)  # nocov


def _get_labeled_artifacts_by_parent_id(
    records: Sequence[OrmModelArtifact],
    related_records: dict[type[OrmBase], Sequence[OrmBase]],
) -> LabeledArtifactsByParentId:
    links = cast(Sequence[OrmNodeLink], related_records[OrmNodeLink])
    artifacts = cast(Sequence[OrmArtifact], [*related_records[OrmArtifact], *records])

    artifacts_by_id = {art.node_id: art for art in artifacts}
    artifacts_by_parent_id: defaultdict[UUID, dict[str, OrmArtifact]] = defaultdict(dict)
    for link in links:
        if link.label is not None:
            artifacts_by_parent_id[link.parent_id][link.label] = artifacts_by_id[link.child_id]

    return artifacts_by_parent_id


def _try_convert_value_to_modeled_type(value: Any) -> GraphModel | Any:
    """Try to convert a value to a modeled type."""
    modeled_type = MODELED_TYPES.get(type(value))
    if modeled_type is not None:
        return modeled_type(value)  # type: ignore
    return value


async def _dump_and_link(graph_obj: GraphBase, parent_id: UUID, label: str) -> Sequence[OrmBase]:
    node, related = await dump_one(graph_obj)
    if not isinstance(node, OrmNode):  # nocov
        msg = f"Expected {graph_obj} to dump an OrmNode, got {node}"
        raise ValueError(msg)
    link = NodeLink(parent_id=parent_id, child_id=node.node_id, label=label)
    return [node, *related, *(await dump_one_flat(link))]
