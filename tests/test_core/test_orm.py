import pytest

from artigraph.core.orm.node import OrmNode


def test_node_with_inconsistent_polymorphic_identity():
    """Test that a node with an inconsistent polymorphic identity raises an error."""
    with pytest.raises(ValueError):

        class MyNode(OrmNode):
            """A node with an inconsistent polymorphic identity."""

            polymorphic_identity = "something"
            __mapper_args__ = {"polymorphic_identity": "something_else"}  # noqa: RUF012