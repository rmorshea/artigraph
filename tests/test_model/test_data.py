# from __future__ import annotations

# from dataclasses import fields
# from typing import Annotated, Any, Optional

# import pandas as pd

# from artigraph.model.base import (
#     write_model,
#     write_models,
# )
# from artigraph.model.data import DataModel
# from artigraph.orm import Node
# from artigraph.serializer.pandas import dataframe_serializer
# from artigraph.storage.file import temp_file_storage

# FileDataFrame = Annotated[pd.DataFrame, dataframe_serializer, temp_file_storage]


# class SampleModel(DataModel, version=1):
#     """A simple artifact model that stores a few basic artifact."""

#     some_value: str
#     remote_value: FileDataFrame
#     inner_model: Optional[None] = None

#     def __eq__(self, other: Any) -> bool:
#         if not isinstance(other, SampleModel):
#             return False
#         for f in fields(self):
#             value = getattr(self, f.name)
#             if isinstance(value, pd.DataFrame):
#                 if not value.equals(getattr(other, f.name)):
#                     return False
#             elif value != getattr(other, f.name):
#                 return False
#         return True


# async def test_save_load_simple_artifact_model():
#     """Test saving and loading a simple artifact model."""
#     artifact = SampleModel(some_value="test-value", remote_value=pd.DataFrame())
#     artifact_id = await write_model("some-label", artifact)
#     loaded_artifact = await _read_model_by_id(artifact_id)
#     assert loaded_artifact == artifact


# async def test_read_write_simple_artifact_model_with_inner_model():
#     """Test saving and loading a simple artifact model with an inner model."""
#     inner_inner_artifact = SampleModel(
#         some_value="inner-inner-value",
#         remote_value=pd.DataFrame(),
#     )
#     inner_artifact = SampleModel(
#         some_value="inner-value",
#         remote_value=pd.DataFrame(),
#         inner_model=inner_inner_artifact,
#     )
#     artifact = SampleModel(
#         some_value="test-value",
#         remote_value=pd.DataFrame(),
#         inner_model=inner_artifact,
#     )

#     artifact_id = await write_model("some-label", artifact)
#     loaded_artifact = await _read_model_by_id(artifact_id)
#     assert loaded_artifact == artifact


# async def test_read_write_child_artifact_models():
#     """Test saving and loading a simple artifact model with child models."""
#     async with create_current(Node):
#         models = {
#             "label": SampleModel(some_value="test-value", remote_value=pd.DataFrame()),
#             "other_label": SampleModel(some_value="other-value", remote_value=pd.DataFrame()),
#             "another_label": SampleModel(some_value="another-value", remote_value=pd.DataFrame()),
#         }
#         await write_models("current", models=models)
#         assert await read_child_models("current", labels=["label", "other_label"]) == {
#             "label": models["label"],
#             "other_label": models["other_label"],
#         }
#         assert await read_child_models("current", labels=["does_not_exist"]) == {}
