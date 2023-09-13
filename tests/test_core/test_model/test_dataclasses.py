from __future__ import annotations

from datetime import datetime, timezone
from typing import Annotated, Any, TypeVar

import pytest

from artigraph.core.api.filter import ModelFilter, NodeFilter, NodeLinkFilter
from artigraph.core.api.link import NodeLink
from artigraph.core.api.node import Node
from artigraph.core.model.base import GraphModel
from artigraph.core.model.dataclasses import dataclass
from artigraph.core.serializer.datetime import datetime_serializer
from artigraph.core.serializer.json import json_serializer
from artigraph.core.storage.file import temp_file_storage
from tests.common.check import check_can_read_write_delete_one

T = TypeVar("T")
DateTime = Annotated[datetime, datetime_serializer]
Json = Annotated[Any, json_serializer]
TempFileStorage = Annotated[T, temp_file_storage]


@dataclass
class SimpleModel(GraphModel, version=1):
    x: int
    y: str
    z: SimpleModel | None = None
    dt: DateTime | None = None
    dt_or_json: DateTime | Json | None = None
    dt_with_storage: TempFileStorage[DateTime | None] = None


def test_dataclass_must_inherit_from_graph_model():
    with pytest.raises(TypeError):

        @dataclass
        class NotAGraphModel:
            pass


@pytest.mark.parametrize(
    "model",
    [
        SimpleModel(x=1, y="2"),
        SimpleModel(x=1, y="2", z=SimpleModel(x=3, y="4", z=SimpleModel(x=5, y="6"))),
        SimpleModel(x=1, y="2", dt=datetime.now(tz=timezone.utc)),
        SimpleModel(x=1, y="2", dt_or_json=datetime.now(tz=timezone.utc)),
        SimpleModel(x=1, y="2", dt_or_json={"a": 1, "b": 2}),
        SimpleModel(x=1, y="2", dt_with_storage=datetime.now(tz=timezone.utc)),
    ],
)
async def test_write_read_delete_dataclass_model(model: GraphModel):
    await check_can_read_write_delete_one(
        model,
        self_filter=ModelFilter(node_id=model.graph_node_id, model_type=SimpleModel),
        related_filters=[
            (Node, NodeFilter(descendant_of=model.graph_node_id)),
            (NodeLink, NodeLinkFilter(ancestor=model.graph_node_id)),
        ],
    )
