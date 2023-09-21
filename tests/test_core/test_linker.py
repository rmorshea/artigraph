from dataclasses import replace
from typing import Annotated, Any

import numpy as np
import pandas as pd
import pytest

from artigraph.core.api.filter import LinkFilter, NodeFilter
from artigraph.core.api.funcs import exists, read, read_one
from artigraph.core.api.link import Link
from artigraph.core.api.node import Node
from artigraph.core.linker import Linker, current_linker, linked
from artigraph.core.serializer.json import json_serializer, json_sorted_serializer
from artigraph.core.storage.file import temp_file_storage
from artigraph.extras.numpy import array_serializer
from artigraph.extras.pandas import dataframe_serializer
from tests.common.model import SimpleDataclassModel


@linked()
async def simple_function(x: int, y: int) -> int:
    return x + y


@linked()
async def function_with_graph_objs(obj: SimpleDataclassModel) -> SimpleDataclassModel:
    return replace(obj, x=obj.x + 1)


@linked()
async def function_with_annotated_serializer(data: Annotated[Any, json_sorted_serializer]) -> Any:
    return {**data, "a": 1}


@linked()
async def call_all() -> None:
    await simple_function(1, 2)
    await function_with_graph_objs(SimpleDataclassModel(x=1, y="2"))
    await function_with_annotated_serializer({"b": 2})


async def test_trace_graph():
    async with Linker(Node()) as root:
        await call_all()
        await call_all()

    root_links = await read.a(Link, LinkFilter(parent=root.node.graph_id))
    assert len(root_links) == 2
    root_links_by_label = {link.label: link for link in root_links}
    assert "call_all[1]" in root_links_by_label
    assert "call_all[2]" in root_links_by_label

    # TODO: test the rest...


def test_trace_sync_graph():
    @linked()
    def add(x: int, y: int) -> int:
        return x + y

    @linked()
    def mul(x: int, y: int) -> int:
        return x * y

    @linked()
    def do_math():
        return add(1, mul(2, 3))

    with Linker(Node()) as root:
        do_math()

    root_links = read.s(Link, LinkFilter(parent=root.node.graph_id))
    assert len(root_links) == 1


def test_trace_with_union_annotated_func():
    @linked()
    def some_func(
        data: Annotated[  # noqa: ARG001
            pd.DataFrame | np.ndarray, array_serializer, dataframe_serializer
        ]
    ) -> Any:
        pass

    with Linker(Node()):
        some_func(pd.DataFrame())


async def test_traced_function_with_node_as_arg():
    @linked()
    def some_func(
        data: Node,  # noqa: ARG001
    ) -> None:
        pass

    async with Linker(Node()) as root:
        inner = Node()
        some_func(inner)

    assert exists.s(Node, NodeFilter(ancestor_of=inner.graph_id, id=root.node.graph_id))


async def test_traced_function_do_not_save():
    @linked(exclude={"data"})
    def some_func(
        data: Node,  # noqa: ARG001
    ) -> None:
        pass

    async with Linker(Node()):
        inner = Node()
        some_func(inner)

    assert not exists.s(Node, NodeFilter(id=inner.graph_id))


async def test_current_node():
    some_func_current_node = None

    @linked()
    def some_func(
        data: Node,  # noqa: ARG001
    ) -> None:
        nonlocal some_func_current_node
        some_func_current_node = current_linker().node

    root = Node()
    async with Linker(root):
        assert current_linker().node is root
        inner = Node()
        some_func(inner)

    actual_some_func_node = await read_one.a(Node, NodeFilter(parent_of=inner.graph_id))
    assert some_func_current_node == actual_some_func_node


def test_link_graph_obj_cannot_have_serializer_or_storage():
    with Linker(Node()) as linker:
        with pytest.raises(ValueError):
            linker.link(Node(), storage=temp_file_storage)
        with pytest.raises(ValueError):
            linker.link(Node(), serializer=json_serializer)


def test_no_duplicate_linker_labels():
    with Linker(Node()) as linker:
        linker.link(Node(), label="test")
        with pytest.raises(ValueError):
            linker.link(Node(), label="test")


def test_link_arbitrary_value():
    with Linker(Node()) as linker:
        linker.link(1)
        linker.link("test")
        linker.link({"test": "data"})
        linker.link(pd.DataFrame())
        linker.link(np.array([1, 2, 3]))
