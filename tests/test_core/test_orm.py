import pytest
from sqlalchemy import ForeignKey
from sqlalchemy.orm import Mapped, mapped_column

from artigraph.core.orm.base import OrmBase, get_fk_dependency_rank
from artigraph.core.orm.node import OrmNode


def test_node_with_inconsistent_polymorphic_identity():
    """Test that a node with an inconsistent polymorphic identity raises an error."""
    with pytest.raises(ValueError):

        class MyNode(OrmNode):
            """A node with an inconsistent polymorphic identity."""

            polymorphic_identity = "something"
            __mapper_args__ = {"polymorphic_identity": "something_else"}  # noqa: RUF012


def test_get_fk_dependency_rank_for_self_reference():
    """Test that get_fk_dependency_rank works for circular relationships."""

    class A(OrmBase):
        __tablename__ = "a"
        pk: Mapped[int] = mapped_column(primary_key=True)
        a_pk: Mapped[int] = mapped_column(ForeignKey("a.pk"))

    assert get_fk_dependency_rank(A) == 0
