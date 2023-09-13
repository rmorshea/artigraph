from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Annotated, Any, TypeVar

import pytest

from artigraph.core.api.filter import ModelFilter, NodeFilter, NodeLinkFilter
from artigraph.core.api.funcs import write_one
from artigraph.core.api.link import NodeLink
from artigraph.core.api.node import Node
from artigraph.core.model.base import GraphModel
from artigraph.core.model.dataclasses import dataclass
from artigraph.core.serializer.datetime import datetime_serializer
from artigraph.core.serializer.json import json_serializer
from artigraph.core.storage.file import FileSystemStorage, temp_file_storage
from tests.common.check import check_can_read_write_delete_one

T = TypeVar("T")
DateTime = Annotated[datetime, datetime_serializer]
Json = Annotated[Any, json_serializer]
TempFileStorage = Annotated[T, temp_file_storage]
tmp_path = TemporaryDirectory()
store1 = FileSystemStorage(Path(tmp_path.name, "store1"))
store2 = FileSystemStorage(Path(tmp_path.name, "store2"))
Store1 = Annotated[T, store1]
Store2 = Annotated[T, store2]


@dataclass
class SimpleModel(GraphModel, version=1):
    x: int
    y: str
    z: SimpleModel | None = None
    dt: DateTime | None = None
    dt_or_json: DateTime | Json | None = None
    dt_with_storage: TempFileStorage[DateTime | None] = None


def test_dataclass_model_must_inherit_from_graph_model():
    with pytest.raises(TypeError):

        @dataclass
        class NotAGraphModel:
            pass


async def test_dataclass_model_one_field_cannot_have_multiple_storages(tmp_path):
    @dataclass
    class ModelFieldHasMultipleStorages(GraphModel, version=1):
        x: Store1[Store2[bytes]]

    with pytest.raises(ValueError):
        await write_one(ModelFieldHasMultipleStorages(x=b"hello"))


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
