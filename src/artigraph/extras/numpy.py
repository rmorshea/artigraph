from __future__ import annotations

import numpy as np
import pandas as pd

from artigraph.core.serializer.base import Serializer
from artigraph.extras.pandas import dataframe_serializer

NP_1D_SHAPE_LEN = 1
NP_2D_SHAPE_LEN = 2


class ArraySerializer(Serializer[np.ndarray]):
    """A serializer for numpy arrays."""

    types = (np.ndarray,)
    name = "artigraph-numpy"

    @staticmethod
    def serialize(value: np.ndarray) -> bytes:
        """Serialize a numpy array."""
        if len(value.shape) == NP_1D_SHAPE_LEN:
            pd_value = pd.DataFrame({"1darray": value})
        elif len(value.shape) == NP_2D_SHAPE_LEN:
            pd_value = pd.DataFrame(dict(enumerate(value.T)))
        else:
            msg = f"Can only serialize 1D or 2D arrays, not {value.shape}."
            raise ValueError(msg)
        return dataframe_serializer.serialize(pd_value)

    @staticmethod
    def deserialize(value: bytes) -> np.ndarray:
        """Deserialize a numpy array."""
        pd_value = dataframe_serializer.deserialize(value)
        if "1darray" in pd_value.columns:
            return pd_value["1darray"].to_numpy()
        return pd_value.to_numpy()


array_serializer = ArraySerializer().register()
"""A serializer for numpy arrays."""
