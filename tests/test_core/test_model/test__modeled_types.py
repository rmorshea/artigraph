import pytest

from artigraph.core.api.funcs import read_one, write_one
from artigraph.core.model._modeled_types import (
    DictModel,
    FrozenSetModel,
    ListModel,
    SetModel,
    TupleModel,
)
from artigraph.core.model.filter import ModelFilter


@pytest.mark.parametrize(
    "data",
    [
        DictModel({"a": 1, "b": 2}),
        SetModel({"a", "b"}),
        FrozenSetModel(frozenset({"a", "b"})),
        ListModel([1, 2]),
        TupleModel((1, 2)),
    ],
)
def test_modeled_types(data):
    write_one(data)
    db_data = read_one(type(data), ModelFilter(id=data.graph_id))
    assert db_data == data
