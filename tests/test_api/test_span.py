import pytest

from artigraph.api.branch import (
    current_node_id,
    read_ancestor_spans,
    read_child_spans,
    read_descendant_spans,
    read_parent_span,
    span_context,
)
from artigraph.orm.group import Group


async def test_span_context():
    """Test the span context."""
    assert current_node_id(allow_none=True) is None
    async with span_context(label="something"):
        assert current_node_id() is not None
    assert current_node_id(allow_none=True) is None


async def test_span_context_nested():
    """Test the span context."""
    assert current_node_id(allow_none=True) is None
    async with span_context(label="outer"):
        outer_span_id = current_node_id()
        async with span_context(label="inner"):
            assert current_node_id() != outer_span_id
        assert current_node_id() == outer_span_id
    assert current_node_id(allow_none=True) is None


async def test_span_context_nested_exception():
    """Test the span context."""
    assert current_node_id(allow_none=True) is None
    with pytest.raises(RuntimeError):
        async with span_context(label="something"):
            assert current_node_id() is not None
            raise RuntimeError()
    assert current_node_id(allow_none=True) is None


def test_get_current_span_id_error_if_no_span():
    with pytest.raises(RuntimeError):
        current_node_id()


async def test_cannot_specify_span_and_label():
    with pytest.raises(ValueError):
        async with span_context(span=Group(node_parent_id=None), label="something"):
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
    if isinstance(spans, Group):
        span_labels = {spans.span_label}
    else:
        span_labels = {s.span_label for s in spans}
    assert span_labels == expected_span_labels
