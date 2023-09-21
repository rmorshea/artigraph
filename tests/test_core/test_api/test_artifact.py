from datetime import datetime, timezone
from typing import Any

import pytest

from artigraph.core.api.artifact import Artifact, SaveSpec
from artigraph.core.api.filter import ArtifactFilter
from artigraph.core.serializer.datetime import datetime_serializer
from artigraph.core.serializer.json import json_serializer
from artigraph.core.storage.file import temp_file_storage
from tests.common.check import check_can_read_write_delete_one


@pytest.mark.parametrize(
    "artifact",
    [
        Artifact(value=b"Hello, world!"),
        Artifact(value={"test": "data"}, serializer=json_serializer),
        Artifact(value={"test": "data"}, serializer=json_serializer, storage=temp_file_storage),
    ],
)
async def test_write_read_delete_database_artifact(artifact: Artifact):
    await check_can_read_write_delete_one(
        artifact,
        self_filter=ArtifactFilter(id=artifact.graph_id),
    )


@pytest.mark.parametrize(
    "spec, value",
    [
        (SaveSpec(), b"Hello, world!"),
        (SaveSpec(serializers=[json_serializer]), {"test": "data"}),
        (
            SaveSpec(serializers=[json_serializer], storage=temp_file_storage),
            {"test": "data"},
        ),
        (
            SaveSpec(
                serializers=[datetime_serializer, json_serializer],
                storage=temp_file_storage,
            ),
            datetime.now(tz=timezone.utc),
        ),
        (
            SaveSpec(
                serializers=[datetime_serializer, json_serializer],
                storage=temp_file_storage,
            ),
            {"test": "data"},
        ),
    ],
)
async def test_write_read_delete_with_save_spec(spec: SaveSpec, value: Any):
    art = spec.create_artifact(value)
    await check_can_read_write_delete_one(
        art,
        self_filter=ArtifactFilter(id=art.graph_id),
    )


def test_create_strict_artifact_with_no_serializers_is_error():
    with pytest.raises(ValueError):
        SaveSpec().create_artifact(["some", "data"], strict=True)


def test_check_save_spec_strict_create_artifact():
    spec = SaveSpec(serializers=[datetime_serializer])
    with pytest.raises(TypeError):
        spec.create_artifact(["some", "data"], strict=True)


def test_unstrict_save_spec():
    spec = SaveSpec(serializers=[datetime_serializer])
    assert spec.create_artifact(["some", "data"], strict=False).serializer is json_serializer
