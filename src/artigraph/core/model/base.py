from __future__ import annotations

from collections import defaultdict
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any, ClassVar, Iterator, Mapping, Sequence, TypedDict, TypeVar, cast
from uuid import UUID

from typing_extensions import Self, TypeAlias

from artigraph import __version__ as artigraph_version
from artigraph.core.api.artifact import SaveSpec, load_deserialized_artifact_value
from artigraph.core.api.filter import Filter, LinkFilter, NodeFilter
from artigraph.core.api.funcs import GraphObject, dump_one, dump_one_flat
from artigraph.core.api.link import Link
from artigraph.core.orm.artifact import (
    OrmArtifact,
    OrmModelArtifact,
)
from artigraph.core.orm.base import OrmBase
from artigraph.core.orm.link import OrmLink
from artigraph.core.orm.node import OrmNode
from artigraph.core.serializer.json import json_sorted_serializer
from artigraph.core.utils.misc import TaskBatch

M = TypeVar("M", bound="GraphModel")

ModelData: TypeAlias = dict[str, tuple[Any, SaveSpec]]
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


class GraphModel(GraphObject[OrmModelArtifact, OrmBase, NodeFilter[Any]]):
    """A base for all modeled artifacts."""

    graph_id: UUID
    """The unique ID of this model."""

    graph_orm_type: ClassVar[type[OrmModelArtifact]] = OrmModelArtifact
    """The ORM type for this model."""

    graph_model_name: ClassVar[str]
    """The name of the artifact model."""

    graph_model_version: ClassVar[int] = 1
    """The version of the artifact model."""

    def graph_model_data(self) -> ModelData:
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
        return NodeFilter(id=self.graph_id)

    @classmethod
    def graph_filter_related(cls, where: NodeFilter[Any]) -> dict[type[OrmBase], Filter]:
        return {
            OrmArtifact: NodeFilter(descendant_of=where),
            OrmLink: LinkFilter(ancestor=where),
        }

    async def graph_dump_self(self) -> OrmModelArtifact:
        metadata_dict = ModelMetadata(artigraph_version=artigraph_version)
        return self._graph_make_own_metadata_artifact(metadata_dict)

    async def graph_dump_related(self) -> Sequence[OrmBase]:
        dump_related: TaskBatch[Sequence[OrmBase]] = TaskBatch()
        for label, (value, spec) in self.graph_model_data().items():
            maybe_model = _try_convert_value_to_modeled_type(value)
            if spec.is_empty() and isinstance(maybe_model, GraphObject):
                dump_related.add(_dump_and_link, maybe_model, self.graph_id, label)
            else:
                art = spec.create_artifact(value)
                dump_related.add(_dump_and_link, art, self.graph_id, label)
        return [r for rs in await dump_related.gather() for r in rs]

    @classmethod
    async def graph_load(
        cls,
        self_records: Sequence[OrmModelArtifact],
        related_records: dict[type[OrmBase], Sequence[OrmBase]],
    ) -> Sequence[Self]:
        arts_dict_by_p_id = _get_labeled_artifacts_by_source_id(self_records, related_records)
        return [
            (
                await model_type._graph_load_from_labeled_artifacts_by_source_id(
                    art, arts_dict_by_p_id
                )
            )
            for art in self_records
            if issubclass(model_type := get_model_type_by_name(art.model_artifact_type_name), cls)
        ]

    @classmethod
    async def _graph_load_from_labeled_artifacts_by_source_id(
        cls,
        model_metadata_artifact: OrmModelArtifact,
        labeled_artifacts_by_source_id: LabeledArtifactsByParentId,
    ) -> Self:
        return cls.graph_model_init(
            ModelInfo(
                graph_id=model_metadata_artifact.id,
                version=model_metadata_artifact.model_artifact_version,
                metadata=await load_deserialized_artifact_value(model_metadata_artifact),
            ),
            await cls._graph_load_kwargs_from_labeled_artifacts_by_source_id(
                model_metadata_artifact,
                labeled_artifacts_by_source_id,
            ),
        )

    @classmethod
    async def _graph_load_kwargs_from_labeled_artifacts_by_source_id(
        cls,
        model_metadata_artifact: OrmModelArtifact,
        labeled_artifacts_by_source_id: LabeledArtifactsByParentId,
    ) -> dict[str, Any]:
        labeled_children = labeled_artifacts_by_source_id[model_metadata_artifact.id]

        load_field_values: TaskBatch[Any] = TaskBatch()
        for child in labeled_children.values():
            if isinstance(child, OrmModelArtifact):
                child_cls = get_model_type_by_name(child.model_artifact_type_name)
                load_field_values.add(
                    child_cls._graph_load_from_labeled_artifacts_by_source_id,
                    child,
                    labeled_artifacts_by_source_id,
                )
            else:
                load_field_values.add(
                    load_deserialized_artifact_value,
                    child,
                )

        return dict(zip(labeled_children.keys(), await load_field_values.gather()))

    def _graph_make_own_metadata_artifact(self, metadata: ModelMetadata) -> OrmModelArtifact:
        return OrmModelArtifact(
            id=self.graph_id,
            artifact_serializer=json_sorted_serializer.name,
            database_artifact_data=json_sorted_serializer.serialize(metadata),
            model_artifact_type_name=self.graph_model_name,
            model_artifact_version=self.graph_model_version,
        )


@dataclass(frozen=True)
class ModelInfo:
    """The info for an artifact model."""

    graph_id: UUID
    """The unique ID of the artifact model."""
    version: int
    """The version of the artifact model."""
    metadata: ModelMetadata
    """The metadata for the artifact model"""


class ModelMetadata(TypedDict):
    """The metadata for an artifact model."""

    artigraph_version: str
    """The version of Artigraph used to generate the model"""


def _get_labeled_artifacts_by_source_id(
    records: Sequence[OrmModelArtifact],
    related_records: dict[type[OrmBase], Sequence[OrmBase]],
) -> LabeledArtifactsByParentId:
    links = cast(Sequence[OrmLink], related_records[OrmLink])
    artifacts = cast(Sequence[OrmArtifact], [*related_records[OrmArtifact], *records])

    artifacts_by_id = {art.id: art for art in artifacts}
    artifacts_by_source_id: defaultdict[UUID, dict[str, OrmArtifact]] = defaultdict(dict)
    for link in links:
        if link.label is None:  # nocov
            msg = f"Model link {link} has no label"
            raise ValueError(msg)
        else:
            artifacts_by_source_id[link.source_id][link.label] = artifacts_by_id[link.target_id]

    return artifacts_by_source_id


def _try_convert_value_to_modeled_type(value: Any) -> GraphModel | Any:
    """Try to convert a value to a modeled type."""
    modeled_type = MODELED_TYPES.get(type(value))
    if modeled_type is not None:
        return modeled_type(value)  # type: ignore
    return value


async def _dump_and_link(graph_obj: GraphObject, source_id: UUID, label: str) -> Sequence[OrmBase]:
    node, related = await dump_one(graph_obj)
    if not isinstance(node, OrmNode):  # nocov
        msg = f"Expected {graph_obj} to dump an OrmNode, got {node}"
        raise ValueError(msg)
    link = Link(source_id=source_id, target_id=node.id, label=label)
    return [node, *related, *(await dump_one_flat(link))]
