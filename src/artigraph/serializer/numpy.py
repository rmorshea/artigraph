import numpy as np
import pandas as pd

from artigraph.serializer._core import register_serializer
from artigraph.serializer.pandas import pandas_serializer

NP_1D_SHAPE_LEN = 1
NP_2D_SHAPE_LEN = 2


class NumpySerializer:
    """A serializer for numpy arrays."""

    types = np.ndarray
    name = "artigraph.numpy"

    @staticmethod
    def serialize(value: np.ndarray) -> bytes:
        """Serialize a numpy array."""
        if len(value.shape) == NP_1D_SHAPE_LEN:
            pd_value = pd.Series(value)
        elif len(value.shape) == NP_2D_SHAPE_LEN:
            pd_value = pd.DataFrame(dict(enumerate(value.T)))
        else:
            msg = f"Can only serialize 1D or 2D arrays, not {value.shape}."
            raise ValueError(msg)
        return pandas_serializer.serialize(pd_value)

    @staticmethod
    def deserialize(value: bytes) -> np.ndarray:
        """Deserialize a numpy array."""
        pd_value = pandas_serializer.deserialize(value)
        if isinstance(pd_value, pd.Series):
            return pd_value.to_numpy()
        elif isinstance(pd_value, pd.DataFrame):
            return pd_value.values.T
        else:
            msg = f"Can only deserialize Series or DataFrame, not {type(pd_value)}."
            raise ValueError(msg)


numpy_serializer = NumpySerializer()
"""A serializer for numpy arrays."""

register_serializer(numpy_serializer)
