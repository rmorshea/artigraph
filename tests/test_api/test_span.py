import pytest

from artigraph.api.span import (
    create_span_artifact,
    create_span_artifacts,
    get_current_span_id,
    read_ancestor_spans,
    read_child_spans,
    read_descendant_spans,
    read_parent_span,
    read_span_artifact,
    read_span_artifacts,
    span_context,
)
from artigraph.orm.span import Span
from tests.test_api.test_artifact_model import SimpleArtifactModel


async def test_span_context():
    """Test the span context."""
    assert get_current_span_id(allow_none=True) is None
    async with span_context(label="something"):
        assert get_current_span_id() is not None
    assert get_current_span_id(allow_none=True) is None


async def test_span_context_nested():
    """Test the span context."""
    assert get_current_span_id(allow_none=True) is None
    async with span_context(label="outer"):
        outer_span_id = get_current_span_id()
        async with span_context(label="inner"):
            assert get_current_span_id() != outer_span_id
        assert get_current_span_id() == outer_span_id
    assert get_current_span_id(allow_none=True) is None


async def test_span_context_nested_exception():
    """Test the span context."""
    assert get_current_span_id(allow_none=True) is None
    with pytest.raises(RuntimeError):
        async with span_context(label="something"):
            assert get_current_span_id() is not None
            raise RuntimeError()
    assert get_current_span_id(allow_none=True) is None


async def test_span_context_create_and_read_one_artifact():
    """Test the span context."""
    async with span_context(label="something") as span:
        artifact = SimpleArtifactModel("test", None)
        await create_span_artifact("current", label="test", artifact=artifact)
        assert await read_span_artifact("current", label="test") == artifact
    assert await read_span_artifact(span.node_id, label="test") == artifact


async def test_span_context_create_and_read_many_artifacts():
    """Test the span context."""
    async with span_context(label="something") as span:
        artifacts = {
            "test1": SimpleArtifactModel("test1", None),
            "test2": SimpleArtifactModel("test2", None),
        }
        await create_span_artifacts("current", artifacts)
        assert await read_span_artifacts("current") == artifacts
    assert await read_span_artifacts(span.node_id) == artifacts


async def test_read_span_artifacts_with_no_artifacts():
    """Test the span context."""
    async with span_context(label="something") as span:
        pass
    assert await read_span_artifacts(span.node_id) == {}


def test_get_current_span_id_error_if_no_span():
    with pytest.raises(RuntimeError):
        get_current_span_id()


async def test_cannot_specify_span_and_label():
    with pytest.raises(ValueError):
        async with span_context(span=Span(node_parent_id=None), label="something"):
            ...


@pytest.mark.parametrize(
    "span_query,span_name,expected_span_labels",
    [
        (read_ancestor_spans, "grandchild_1", {"parent", "child_1"}),
        (read_child_spans, "parent", {"child_1", "child_2"}),
        (read_descendant_spans, "parent", {"child_1", "child_2", "grandchild_1", "grandchild_2"}),
        (read_parent_span, "child_1", {"parent"}),
    ],
)
async def test_span_queries(span_query, span_name, expected_span_labels):
    async with span_context(label="parent") as parent:
        async with span_context(label="child_1") as child_1:
            async with span_context(label="grandchild_1") as grandchild_1:
                ...
            async with span_context(label="grandchild_2") as grandchild_2:
                ...
        async with span_context(label="child_2") as child_2:
            ...

    span_ids_by_name = {
        "parent": parent.node_id,
        "child_1": child_1.node_id,
        "grandchild_1": grandchild_1.node_id,
        "grandchild_2": grandchild_2.node_id,
        "child_2": child_2.node_id,
    }

    spans = await span_query(span_ids_by_name[span_name])
    if isinstance(spans, Span):
        span_labels = {spans.span_label}
    else:
        span_labels = {s.span_label for s in spans}
    assert span_labels == expected_span_labels
