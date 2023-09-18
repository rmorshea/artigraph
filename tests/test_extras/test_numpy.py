import numpy as np
import pytest

from artigraph.extras.numpy import array_serializer


def test_serialize_deserialize_1d_array():
    array_1d = np.array([1, 2, 3])
    serialized = array_serializer.serialize(array_1d)
    assert all(array_serializer.deserialize(serialized) == array_1d)


def test_serialize_deserialize_2d_array():
    array_2d = np.array([[1, 2, 3], [4, 5, 6]])
    serialized = array_serializer.serialize(array_2d)
    assert all(all(mask) for mask in (array_serializer.deserialize(serialized) == array_2d))


def test_cannot_serialize_higher_dimensional_array():
    array_3d = np.array([[[1, 2, 3], [4, 5, 6]]])
    with pytest.raises(ValueError, match=r"Can only serialize 1D or 2D arrays, not"):
        array_serializer.serialize(array_3d)
