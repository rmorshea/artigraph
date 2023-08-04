import pytest

from artigraph.api.span import (
    create_span_artifact,
    create_span_artifacts,
    get_current_span_id,
    read_span_artifact,
    read_span_artifacts,
    span_context,
)
from tests.test_api.test_artifact_model import SimpleArtifactModel


async def test_span_context():
    """Test the span context."""
    assert get_current_span_id(allow_none=True) is None
    async with span_context():
        assert get_current_span_id() is not None
    assert get_current_span_id(allow_none=True) is None


async def test_span_context_nested():
    """Test the span context."""
    assert get_current_span_id(allow_none=True) is None
    async with span_context():
        outer_span_id = get_current_span_id()
        async with span_context():
            assert get_current_span_id() != outer_span_id
        assert get_current_span_id() == outer_span_id
    assert get_current_span_id(allow_none=True) is None


async def test_span_context_nested_exception():
    """Test the span context."""
    assert get_current_span_id(allow_none=True) is None
    with pytest.raises(RuntimeError):
        async with span_context():
            assert get_current_span_id() is not None
            raise RuntimeError()
    assert get_current_span_id(allow_none=True) is None


async def test_span_context_create_and_read_one_artifact():
    """Test the span context."""
    async with span_context() as span:
        artifact = SimpleArtifactModel("test", None)
        await create_span_artifact("current", label="test", artifact=artifact)
        assert await read_span_artifact("current", label="test") == artifact
    assert await read_span_artifact(span.node_id, label="test") == artifact


async def test_span_context_create_and_read_many_artifacts():
    """Test the span context."""
    async with span_context() as span:
        artifacts = {
            "test1": SimpleArtifactModel("test1", None),
            "test2": SimpleArtifactModel("test2", None),
        }
        await create_span_artifacts("current", artifacts)
        assert await read_span_artifacts("current") == artifacts
    assert await read_span_artifacts(span.node_id) == artifacts
