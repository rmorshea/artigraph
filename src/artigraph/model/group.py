from __future__ import annotations

from contextvars import ContextVar, Token
from typing import Any, Generic, TypeVar

from pydantic import BaseModel
from typing_extensions import Self

from artigraph.api.node import write_node
from artigraph.model.base import write_models
from artigraph.orm.node import Node

N = TypeVar("N", bound=Node)

_CURRENT_MODEL_GROUP: ContextVar[ModelGroup[Node]] = ContextVar()


def current_model_group() -> ModelGroup[Node]:
    """Get the current model group."""
    return _CURRENT_MODEL_GROUP.get()


class ModelGroup(Generic[N]):
    """A group of models that are written to the database together

    Parameters:
        node: The node that the models belong to
        rollback: If True, the models will not be written to the database if an exception is raised
    """

    _current_model_group_token: Token[ModelGroup[Node]]

    def __init__(self, node: N, *, rollback: bool = False) -> None:
        self.node = node
        self.rollback = rollback
        self._models: dict[str, BaseModel] = {}

    async def __aenter__(self) -> Self:
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
        return self.add_models({label: model}, replace_existing=replace_existing)

    async def __aexit__(self, exc_type: type[Exception] | None, *exc: Any) -> None:
        _CURRENT_MODEL_GROUP.reset(self._current_model_group_token)
        if exc_type is not None:
            if self.rollback:
                return
        await write_models(parent_id=self.node.node_id, models=self._models)
