from __future__ import annotations

from contextvars import ContextVar
from typing import Any, Sequence

from typing_extensions import Self

from artigraph.core.api.base import GraphBase
from artigraph.core.api.filter import NodeLinkFilter, ValueFilter
from artigraph.core.api.funcs import delete, read, write
from artigraph.core.api.link import NodeLink
from artigraph.core.api.node import Node
from artigraph.core.model.base import GraphModel
from artigraph.core.model.filter import ModelFilter
from artigraph.core.utils.anysync import AnySynContextManager, anysyncmethod

_CURRENT_MODEL_GROUP: ContextVar[ModelGroup | None] = ContextVar(
    "CURRENT_MODEL_GROUP",
    default=None,
)


def current_group() -> ModelGroup:
    """Get the current model group."""
    group = _CURRENT_MODEL_GROUP.get()
    if group is None:
        msg = "No current model group"
        raise RuntimeError(msg)
    return group


class ModelGroup(AnySynContextManager["ModelGroup"]):
    """A group of models that are children of a node."""

    def __init__(self, node: Node, label: str | None = None) -> None:
        self.node = node
        self.label = label
        self._models: dict[str, GraphModel] = {}

    def add_model(self, label: str, model: GraphModel) -> None:
        """Add a model to this group."""
        self.add_models({label: model})

    def add_models(self, models: dict[str, GraphModel]) -> None:
        """Add models to this group."""
        label_intersetion = set(models).intersection(self._models)
        if label_intersetion:
            msg = f"Models with labels {list(label_intersetion)} already exist in this group"
            raise ValueError(msg)
        self._models.update(models)

    @anysyncmethod
    async def get_model(self, label: str, *, fresh: bool = False) -> GraphModel:
        """Get a model from this group."""
        return (await self.get_models.a([label], fresh=fresh))[label]

    @anysyncmethod
    async def get_models(
        self,
        labels: Sequence[str] | None = None,
        *,
        fresh: bool = False,
    ) -> dict[str, GraphModel]:
        """Get models from this group - if a label is not in the group, it will be ignored."""
        if fresh:
            await self._refresh_models(labels)
        labels = [l for l in labels if l in self._models] if labels else tuple(self._models)
        return {l: self._models[l] for l in labels}

    @anysyncmethod
    async def delete_model(self, label: str) -> None:
        """Delete a model from this group."""
        return await self.delete_models.a([label])

    @anysyncmethod
    async def delete_models(self, labels: Sequence[str] | None = None) -> None:
        """Delete models from this group."""
        await delete.a(GraphModel, ModelFilter(child_of=self.node.node_id, labels=labels))

    async def _refresh_models(self, labels: Sequence[str] | None = None) -> None:
        node_links = await read.a(
            NodeLink,
            NodeLinkFilter(
                parent=self.node.node_id,
                label=ValueFilter(in_=labels, is_not=None),
            ),
        )
        child_ids = [link.child_id for link in node_links]
        labels_by_id = {link.child_id: link.label for link in node_links if link.label is not None}
        model_list = await read.a(GraphModel, ModelFilter(node_id=ValueFilter(in_=child_ids)))
        self._models.update({labels_by_id[m.graph_node_id]: m for m in model_list})

    async def _enter(self) -> Self:
        parent_group = _CURRENT_MODEL_GROUP.get()
        objs: list[GraphBase] = [self.node]
        if parent_group is not None:
            objs.append(
                NodeLink(
                    child_id=self.node.node_id,
                    parent_id=parent_group.node.node_id,
                    label=self.label,
                )
            )
        await write.a(objs)
        await self._refresh_models()
        return self

    async def _exit(self, *_: Any) -> None:
        objs_to_create: list[GraphBase] = []
        for label, model in self._models.items():
            objs_to_create.append(
                NodeLink(
                    child_id=model.graph_node_id,
                    parent_id=self.node.node_id,
                    label=label,
                )
            )
            objs_to_create.append(model)
        await write.a(objs_to_create)
