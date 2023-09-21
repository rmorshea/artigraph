from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Annotated, Any, TypeVar

import pytest

from artigraph.core.api.artifact import SaveSpec
from artigraph.core.api.filter import LinkFilter, NodeFilter
from artigraph.core.api.funcs import write_one
from artigraph.core.api.link import Link
from artigraph.core.api.node import Node
from artigraph.core.model.base import GraphModel
from artigraph.core.model.dataclasses import dataclass, get_annotated_model_data
from artigraph.core.model.filter import ModelFilter
from artigraph.core.serializer.json import json_sorted_serializer
from artigraph.core.storage.file import FileSystemStorage
from tests.common.check import check_can_read_write_delete_one
from tests.common.model import SimpleDataclassModel

T = TypeVar("T")
tmp_path = TemporaryDirectory()
store1 = FileSystemStorage(Path(tmp_path.name, "store1"))
store2 = FileSystemStorage(Path(tmp_path.name, "store2"))
Store1 = Annotated[T, store1]
Store2 = Annotated[T, store2]


def test_dataclass_model_must_inherit_from_graph_model():
    with pytest.raises(TypeError):

        @dataclass
        class NotAGraphModel:
            pass


async def test_dataclass_model_one_field_cannot_have_multiple_storages():
    @dataclass
    class ModelFieldHasMultipleStorages(GraphModel, version=1):
        x: Store1[Store2[bytes]]

    with pytest.raises(ValueError):
        await write_one.a(ModelFieldHasMultipleStorages(x=b"hello"))


@pytest.mark.parametrize(
    "model",
    [
        SimpleDataclassModel(x=1, y="2"),
        SimpleDataclassModel(
            x=1,
            y="2",
            z=SimpleDataclassModel(x=3, y="4", z=SimpleDataclassModel(x=5, y="6")),
        ),
        SimpleDataclassModel(x=1, y="2", dt=datetime.now(tz=timezone.utc)),
        SimpleDataclassModel(x=1, y="2", dt_or_json=datetime.now(tz=timezone.utc)),
        SimpleDataclassModel(x=1, y="2", dt_or_json={"a": 1, "b": 2}),
        SimpleDataclassModel(x=1, y="2", dt_with_storage=datetime.now(tz=timezone.utc)),
    ],
)
async def test_write_read_delete_dataclass_model(model: GraphModel):
    await check_can_read_write_delete_one(
        model,
        self_filter=ModelFilter(id=model.graph_id, model_type=SimpleDataclassModel),
        related_filters=[
            (Node, NodeFilter(descendant_of=model.graph_id)),
            (Link, LinkFilter(ancestor=model.graph_id)),
        ],
    )


def test_get_annotated_model_data():
    @dataclass
    class SomeModelWithAnnotatedData(GraphModel, version=1):
        x: Annotated[Any, store1]
        y: Annotated[Any, json_sorted_serializer]

    x = object()
    y = object()

    assert get_annotated_model_data(SomeModelWithAnnotatedData(x=x, y=y), ["x", "y"]) == {
        "x": (x, SaveSpec(storage=store1, serializers=[])),
        "y": (y, SaveSpec(serializers=[json_sorted_serializer])),
    }
