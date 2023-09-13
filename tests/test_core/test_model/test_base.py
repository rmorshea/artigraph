from __future__ import annotations

import pytest

from artigraph.core.model.base import MODELED_TYPES, _try_convert_value_to_modeled_type


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
    assert value == model_type.graph_model_init(model_type.graph_model_version, kwargs)
