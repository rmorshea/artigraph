from __future__ import annotations

from uuid import uuid1

import pytest

from artigraph import __version__ as artigraph_version
from artigraph.core.model.base import (
    MODELED_TYPES,
    GraphModel,
    ModelInfo,
    ModelMetadata,
    _try_convert_value_to_modeled_type,
)


@pytest.mark.parametrize(
    "value",
    [
        {"some": "data"},
        ("some", "data"),
        ["some", "data"],
        frozenset({"some", "data"}),
        {"some", "data"},
    ],
)
def test_try_convert_value_to_and_from_modeled_type(value):
    kwargs = {
        k: v for k, (v, _) in _try_convert_value_to_modeled_type(value).graph_model_data().items()
    }
    model_type = MODELED_TYPES[type(value)]
    model_info = ModelInfo(
        graph_id=uuid1(),
        version=model_type.graph_model_version,
        metadata=ModelMetadata(artigraph_version=artigraph_version),
    )
    assert value == model_type.graph_model_init(model_info, kwargs)


def test_cannot_define_model_with_same_name():
    class SomeModelName(GraphModel, version=1):  # type: ignore
        pass

    with pytest.raises(RuntimeError):

        class SomeModelName(GraphModel, version=1):  # noqa: F811
            pass
