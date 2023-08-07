import pytest

from artigraph.model.base import (
    MODELED_TYPES,
    BaseModel,
    get_model_type_by_name,
    try_convert_value_to_modeled_type,
)


def _get_model_date(model):
    data = {}
    for k, (v, _) in model.model_data().items():
        if isinstance(v, BaseModel):
            data[k] = _get_model_date(v)
        else:
            data[k] = v
    return data


def test_get_model_type_by_name_unknown_name():
    """Test the get_model_type_by_name function."""
    with pytest.raises(ValueError):
        assert get_model_type_by_name("unknown") is None


@pytest.mark.parametrize(
    "value",
    [
        {"some": "data"},
        ("some", "data"),
        ["some", "data"],
        frozenset(["some", "data"]),
        {"some", "data"},
    ],
)
def test_try_convert_value_to_and_from_modeled_type(value):
    kwargs = {k: v for k, (v, _) in try_convert_value_to_modeled_type(value).model_data().items()}
    model_type = MODELED_TYPES[type(value)]
    assert value == model_type.model_init(model_type.model_version, kwargs)
