import pytest

from artigraph.model.base import get_model_type_by_name


def test_get_model_type_by_name_unknown_name():
    """Test the get_model_type_by_name function."""
    with pytest.raises(ValueError):
        assert get_model_type_by_name("unknown") is None
