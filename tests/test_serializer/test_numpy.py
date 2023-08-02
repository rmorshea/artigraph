import numpy as np

from artigraph.serializer.numpy import numpy_serializer


def test_serialize_deserialize_1d_array():
    array_1d = np.array([1, 2, 3])
    serialized = numpy_serializer.serialize(array_1d)
    assert all(numpy_serializer.deserialize(serialized) == array_1d)


def test_serialize_deserialize_2d_array():
    array_2d = np.array([[1, 2, 3], [4, 5, 6]])
    serialized = numpy_serializer.serialize(array_2d)
    assert all(all(mask) for mask in (numpy_serializer.deserialize(serialized) == array_2d))


def test_cannot_serialize_higher_dimensional_array():
    array_3d = np.array([[[1, 2, 3], [4, 5, 6]]])
    try:
        numpy_serializer.serialize(array_3d)
    except ValueError:
        pass
    else:
        msg = "Should not be able to serialize a 3D array."
        raise AssertionError(msg)
