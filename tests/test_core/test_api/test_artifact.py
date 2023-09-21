import pytest

from artigraph.core.api.artifact import Artifact
from artigraph.core.api.filter import ArtifactFilter
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
