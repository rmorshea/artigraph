from __future__ import annotations

from contextvars import ContextVar, Token
from typing import Any, Generic, Sequence, TypeVar

from pydantic import BaseModel
from typing_extensions import Self

from artigraph.api.filter import NodeRelationshipFilter, ValueFilter
from artigraph.api.node import read_node_or_none, read_nodes_exist, write_node
from artigraph.model.base import delete_models, read_models, write_models
from artigraph.model.filter import ModelFilter
from artigraph.orm.node import Node

N = TypeVar("N", bound=Node)

_CURRENT_MODEL_GROUP: ContextVar[ModelGroup[Node]] = ContextVar("CURRENT_MODEL_GROUP")


def current_model_group() -> ModelGroup[Node]:
    """Get the current model group."""
    return _CURRENT_MODEL_GROUP.get()


def current_model_group_or_none() -> ModelGroup[Node]:
    """Get the current model group, or return None if none exists"""
    try:
        return current_model_group()
    except LookupError:
        return None


class ModelGroup(Generic[N]):
    """A group of models that are written to the database together

    Parameters:
        node: The node that the models belong to
        rollback: If True, the models will not be written to the database if an exception is raised.
    """

    _current_model_group_token: Token[ModelGroup[Node]]
    node: N

    def __init__(self, node: N | int) -> None:
        self._node_id = _LazyNodeId(node, current_model_group_or_none())
        self._models: dict[str, BaseModel] = {}

    def add_models(self, models: dict[str, BaseModel]) -> None:
        """Add models to the group."""
        label_intersetion = set(models.keys()).intersection(self._models.keys())
        if label_intersetion:
            msg = f"Models with labels {list(label_intersetion)} already exist in this group"
            raise ValueError(msg)
        self._models.update(models)

    def add_model(self, label: str, model: BaseModel) -> None:
        """Add a model to the group."""
        return self.add_models({label: model})

    async def get_models(
        self,
        labels: Sequence[str] | None = None,
        *,
        fresh: bool = False,
    ) -> dict[str, BaseModel]:  # nocov (FIXME: this is covered but not detected)
        """Read this group's models from the database."""
        artifact_label_filter = self._labels_to_refresh(labels, fresh=fresh)
        if artifact_label_filter:
            self._models.update(
                {
                    qual.artifact.artifact_label: qual.value
                    for qual in await read_models(
                        ModelFilter(
                            relationship=NodeRelationshipFilter(child_of=await self._node_id.get()),
                            artifact_label=artifact_label_filter,
                        )
                    )
                }
            )

        return self._models.copy()

    async def get_model(self, label: str, *, fresh: bool = False) -> BaseModel:
        """Read this group's model from the database."""
        return (await self.get_models([label], fresh=fresh))[label]

    async def has_model(self, label: str, *, fresh: bool = False) -> bool:
        """Check if this group has a model with the given label."""
        return await self.has_models([label], fresh=fresh)

    async def has_models(
        self,
        labels: Sequence[str] | None = None,
        *,
        fresh: bool = False,
    ) -> bool:
        """Check if this group has models with the given labels."""
        artifact_label_filter = self._labels_to_refresh(labels, fresh=fresh)
        if artifact_label_filter:
            return await read_nodes_exist(
                ModelFilter(
                    relationship=NodeRelationshipFilter(child_of=await self._node_id.get()),
                    artifact_label=artifact_label_filter,
                )
            )
        return all(label in self._models for label in labels)

    async def remove_model(self, label: str) -> None:
        """Delete this group's model from the database."""
        await self.remove_models([label])

    async def remove_models(self, labels: Sequence[str] | None = None) -> None:
        """Delete the specified models, or all models, from this group in database"""
        await delete_models(
            ModelFilter(
                relationship=NodeRelationshipFilter(child_of=await self._node_id.get()),
                artifact_label=ValueFilter(in_=labels),
            )
        )
        if labels is None:
            self._models.clear()
        else:
            for label in labels:
                self._models.pop(label, None)

    async def get_parent_group(self) -> ModelGroup[Node] | None:
        """Get this groups' parent."""
        node_filter = NodeRelationshipFilter(parent_of=await self._node_id.get())
        parent_node = await read_node_or_none(node_filter)
        return None if parent_node is None else ModelGroup(parent_node)

    async def save(self) -> None:
        """Write the models to the database."""
        await write_models(parent_id=await self._node_id.get(), models=self._models)

    async def __aenter__(self) -> Self:
        self._current_model_group_token = _CURRENT_MODEL_GROUP.set(self)
        return self

    async def __aexit__(self, *exc: Any) -> None:
        _CURRENT_MODEL_GROUP.reset(self._current_model_group_token)
        await self.save()

    def _labels_to_refresh(
        self,
        labels: Sequence[str] | None,
        *,
        fresh: bool,
    ) -> ValueFilter | None:
        labels_to_refresh = (
            labels
            if (
                # if refresh load all given labels
                fresh
                # labels=None is equivalent to all labels
                or labels is None
            )
            else set(labels).difference(self._models.keys())
        )
        if labels_to_refresh is None or labels_to_refresh:
            return ValueFilter(in_=labels_to_refresh)
        return None


class _LazyNodeId:
    _node_id: int

    def __init__(self, node: Node | int, parent_group: ModelGroup | None) -> None:
        self._given = node
        self._parent_group = parent_group

    async def get(self) -> int:
        node_id = getattr(self, "_node_id", None)
        if node_id is not None:
            return node_id
        if isinstance(self._given, int):
            self._node_id = self._given
            return self._given
        if self._given.node_id is None:
            if self._parent_group is not None:
                self._given.node_parent_id = await self._parent_group._node_id.get()
            await write_node(self._given)
        self._node_id = self._given.node_id
        return self._node_id
