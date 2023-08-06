# from __future__ import annotations

# from dataclasses import dataclass, field
# from typing import Any

# from artigraph.api.artifact_model import (
#     ArtifactMapping,
#     DataModel,
#     ArtifactSequence,
#     artifact_field,
# )
# from artigraph.serializer.json import json_serializer
# from artigraph.storage.file import temp_file_storage


# @dataclass(frozen=True)
# class SimpleDataModel(DataModel, version=1):
#     """A simple artifact model that stores a few basic artifact."""

#     some_value: str
#     remote_value: Any = artifact_field(serializer=json_serializer, storage=temp_file_storage)
#     inner_model: None | SimpleDataModel = None


# async def test_save_load_simple_artifact_model():
#     """Test saving and loading a simple artifact model."""
#     artifact = SimpleDataModel(some_value="test-value", remote_value={"some": "data"})
#     artifact_id = await artifact.model_create(None)
#     loaded_artifact = await SimpleDataModel.model_read(artifact_id)
#     assert loaded_artifact == artifact


# async def test_save_load_simple_artifact_model_with_inner_model():
#     """Test saving and loading a simple artifact model with an inner model."""
#     inner_inner_artifact = SimpleDataModel(
#         some_value="inner-inner-value",
#         remote_value={"inner-inner": "data"},
#     )
#     inner_artifact = SimpleDataModel(
#         some_value="inner-value",
#         remote_value={"inner": "data"},
#         inner_model=inner_inner_artifact,
#     )
#     artifact = SimpleDataModel(
#         some_value="test-value",
#         remote_value={"some": "data"},
#         inner_model=inner_artifact,
#     )

#     artifact_id = await artifact.model_create(None)
#     loaded_artifact = await SimpleDataModel.model_read(artifact_id)
#     assert loaded_artifact == artifact


# @dataclass(frozen=True)
# class ComplexDataModel(DataModel, version=1):
#     """A complex artifact model that stores a few basic artifact."""

#     simple: SimpleDataModel
#     mapping: ArtifactMapping[ComplexDataModel] = field(default_factory=dict)
#     sequence: ArtifactSequence[ComplexDataModel] = field(default_factory=dict)


# async def test_save_load_complex_artifact_model():
#     """Test saving and loading a complex artifact model."""
#     simple = SimpleDataModel(some_value="test-value", remote_value={"some": "data"})
#     artifact = ComplexDataModel(
#         simple=simple,
#         mapping=ArtifactMapping(
#             key1=ComplexDataModel(
#                 simple=simple,
#                 mapping=ArtifactMapping(key1=ComplexDataModel(simple=simple)),
#             ),
#             key2=ComplexDataModel(simple=simple),
#         ),
#         sequence=ArtifactSequence(
#             [
#                 ComplexDataModel(simple=simple),
#                 ComplexDataModel(
#                     simple=simple,
#                     mapping=ArtifactMapping(key3=ComplexDataModel(simple=simple)),
#                 ),
#             ]
#         ),
#     )

#     artifact_id = await artifact.model_create(None)
#     loaded_artifact = await ComplexDataModel.model_read(artifact_id)
#     assert loaded_artifact == artifact
