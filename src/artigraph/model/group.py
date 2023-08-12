from __future__ import annotations

from contextvars import ContextVar, Token
from typing import Any, Generic, Sequence, TypeVar
from xml.dom import NodeFilter

from pydantic import BaseModel
from typing_extensions import Self

from artigraph.api.filter import NodeRelationshipFilter, ValueFilter
from artigraph.api.node import read_node, read_node_or_none, write_node
from artigraph.model.base import delete_models, read_model, read_models, write_models
from artigraph.model.filter import ModelFilter
from artigraph.orm.node import Node

N = TypeVar("N", bound=Node)

_CURRENT_MODEL_GROUP: ContextVar[ModelGroup[Node]] = ContextVar("CURRENT_MODEL_GROUP")


def current_model_group() -> ModelGroup[Node]:
    """Get the current model group."""
    return _CURRENT_MODEL_GROUP.get()


class ModelGroup(Generic[N]):
    """A group of models that are written to the database together

    Parameters:
        node: The node that the models belong to
        rollback: If True, the models will not be written to the database if an exception is raised.
    """

    _current_model_group_token: Token[ModelGroup[Node]]
    node: N

    def __init__(self, node: N | int, *, rollback: bool = False) -> None:
        self.rollback = rollback
        self._models: dict[str, BaseModel] = {}
        self._given_node = node

    async def __aenter__(self) -> Self:
        if isinstance(self._given_node, int):
            self.node = await read_node(NodeFilter(node_id=self._given_node))
        else:
            self.node = self._given_node
            if self.node.node_id is None:
                await write_node(self.node)
        self._current_model_group_token = _CURRENT_MODEL_GROUP.set(self)
        return self

    def add_models(
        self,
        models: dict[str, BaseModel],
        *,
        replace_existing: bool = False,
    ) -> None:
        """Add models to the group."""
        if not replace_existing:
            label_intersetion = set(models.keys()).intersection(self._models.keys())
            if label_intersetion:
                msg = f"Models with labels {label_intersetion} already exist in this group"
                raise ValueError(msg)
        self._models.update(models)

    def add_model(
        self,
        label: str,
        model: BaseModel,
        *,
        replace_existing: bool = False,
    ) -> None:
        """Add a model to the group."""
        return self.add_models({label: model}, replace_existing=replace_existing)

    async def read_models(
        self,
        labels: Sequence[str],
        *,
        refresh: bool = False,
    ) -> dict[str, BaseModel]:
        """Read this group's models from the database."""
        labels_to_read = set(labels).difference(self._models.keys()) if not refresh else labels
        self._models.update(
            {
                qual.artifact.artifact_label: qual.value
                for qual in await read_models(
                    ModelFilter(
                        relationship=NodeRelationshipFilter(child_of=self._node_id),
                        artifact_label=ValueFilter(in_=labels_to_read),
                    )
                )
            }
        )
        return self._models.copy()

    async def read_model(self, label: str, *, refresh: bool = False) -> BaseModel:
        """Read this group's model from the database."""
        if label not in self._models or refresh:
            qual = await read_model(
                ModelFilter(
                    relationship=NodeRelationshipFilter(child_of=self._node_id),
                    artifact_label=ValueFilter(eq=label),
                )
            )
            self._models[label] = qual.value
        return self._models[label]

    async def remove_model(self, label: str) -> None:
        """Delete this group's model from the database."""
        await self.remove_models([label])

    async def remove_models(self, labels: Sequence[str] | None = None) -> None:
        """Delete the specified models, or all models, from this group in database"""
        await delete_models(
            ModelFilter(
                relationship=NodeRelationshipFilter(child_of=self._node_id),
                artifact_label=ValueFilter(in_=labels),
            )
        )
        if labels is None:
            self._models.clear()
        else:
            for label in labels:
                del self._models[label]

    async def parent_group(self) -> ModelGroup[Node] | None:
        """Get this groups' parent."""
        parent_node = read_node_or_none(NodeRelationshipFilter(child_of=self._node_id))
        if parent_node is None:
            return None
        return ModelGroup(parent_node)

    async def save(self) -> None:
        """Write the models to the database."""
        await write_models(parent_id=self.node.node_id, models=self._models)

    async def __aexit__(self, exc_type: type[Exception] | None, *exc: Any) -> None:
        _CURRENT_MODEL_GROUP.reset(self._current_model_group_token)
        if exc_type is not None:
            if self.rollback:
                return
        await self.save()

    @property
    def _node_id(self) -> int:
        node_id = (
            self._given_node if isinstance(self._given_node, int) else self._given_node.node_id
        )
        if node_id is None:
            msg = "Node has not been written to the database"
            raise ValueError(msg)
        return node_id
