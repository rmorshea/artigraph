from __future__ import annotations

import operator
from dataclasses import dataclass, field, fields, replace
from datetime import datetime
from typing import TYPE_CHECKING, Any, Callable, Generic, Sequence, TypeVar

from sqlalchemy import BinaryExpression, join, select
from sqlalchemy.orm import aliased
from sqlalchemy.orm.attributes import InstrumentedAttribute
from sqlalchemy.sql.selectable import Select
from typing_extensions import ParamSpec, Self, dataclass_transform

from artigraph.orm import BaseArtifact, Node
from artigraph.serializer.core import Serializer

P = ParamSpec("P")
T = TypeVar("T")
N = TypeVar("N", bound=Node)
A = TypeVar("A", bound=BaseArtifact)


@dataclass_transform()
class _FilterMeta(type):
    def __new__(
        cls,
        name: str,
        bases: tuple[type[Any, ...]],
        namespace: dict[str, Any],
        **kwargs: Any,
    ):
        self = super().__new__(cls, name, bases, namespace, **kwargs)
        self = dataclass(**kwargs)(self)
        return self


class Filter(metaclass=_FilterMeta):
    """Base class for where clauses."""

    def apply(self, query: Select, /) -> Select:
        """Apply the where clause to the given query."""
        raise NotImplementedError()


class GenericFilter(Filter, Generic[T]):
    """Base class for filters that apply generally to a given value."""

    if TYPE_CHECKING:
        # type checkers assume T.__get__ if T is a descriptor - force it to just be T

        @property
        def value(self) -> T:  # type: ignore
            """The specific value to filter by."""

    else:
        value: T = field(init=False)

    def using(self, value: T) -> Self:
        """Return a new filter that uses the given value."""
        new = replace(self)
        new.value = value  # type: ignore
        return new

    def apply(self, query: Select) -> Select:
        raise NotImplementedError()


class NodeFilter(Filter, Generic[N]):
    """Filter nodes that meet the given conditions"""

    node_id: ComparisonFilter[int] | None = None
    """Nodes must have one of these IDs."""

    node_type: NodeTypeFilter[N] | None = None
    """Nodes must be one of these types."""

    is_child_of: Sequence[int] = ()
    """Nodes must be a child of one of nodes with these ids."""

    is_parent_of: Sequence[int] = ()
    """Nodes must be a parent to any one of nodes with these ids."""

    is_descendant_of: Sequence[int] = ()
    """Nodes must be a descendant of one of nodes with these ids."""

    is_ancestor_of: Sequence[int] = ()
    """Nodes must be an ancestor of one of nodes with these ids."""

    created_at: ComparisonFilter[datetime] | None = None
    """Filter nodes by their creation time."""

    updated_at: ComparisonFilter[datetime] | None = None
    """Filter nodes by their last update time."""

    def apply(self, query: Select) -> Select:
        if self.node_id:
            query = self.node_id.using(Node.node_id).apply(query)

        if self.node_type:
            query = self.node_type.apply(query)

        if self.is_child_of:
            query = query.where(Node.node_parent_id.in_(n for n in self.is_child_of))

        if self.is_parent_of:
            query = query.where(
                Node.node_id.in_(
                    select(Node.node_parent_id)
                    .where(Node.node_id.in_(n for n in self.is_parent_of))
                    .alias("parent_ids")
                )
            )

        if self.is_descendant_of:
            descendant_node_cte = (
                select(Node.node_id.label("descendant_id"), Node.node_parent_id)
                .where(Node.node_id.in_(self.is_descendant_of))
                .cte(name="descendants", recursive=True)
            )

            # Recursive case: select the children of the current nodes
            parent_node = aliased(Node)
            descendant_node_cte = descendant_node_cte.union_all(
                select(parent_node.node_id, parent_node.node_parent_id).where(
                    parent_node.node_parent_id == descendant_node_cte.c.descendant_id
                )
            )

            # Join the CTE with the actual Node table to get the descendants
            query = (
                query.select_from(
                    join(
                        Node,
                        descendant_node_cte,
                        Node.node_id == descendant_node_cte.c.descendant_id,
                    )
                )
                # Exclude the roots
                .where(descendant_node_cte.c.descendant_id.notin_(self.is_descendant_of))
            )

        if self.is_ancestor_of:
            # Create a CTE to get the ancestors recursively
            ancestor_node_cte = (
                select(Node.node_id.label("ancestor_id"), Node.node_parent_id)
                .where(Node.node_id.in_(self.is_ancestor_of))
                .cte(name="ancestors", recursive=True)
            )

            # Recursive case: select the parents of the current nodes
            parent_node = aliased(Node)
            ancestor_node_cte = ancestor_node_cte.union_all(
                select(parent_node.node_id, parent_node.node_parent_id).where(
                    parent_node.node_id == ancestor_node_cte.c.node_parent_id
                )
            )

            # Join the CTE with the actual Node table to get the ancestors
            query = (
                query.select_from(
                    join(Node, ancestor_node_cte, Node.node_id == ancestor_node_cte.c.ancestor_id)
                )
                # Exclude the root node itself
                .where(ancestor_node_cte.c.ancestor_id.notin_(self.is_ancestor_of))
            )

        if self.created_at:
            query = self.created_at.using(Node.node_created_at).apply(query)

        if self.updated_at:
            query = self.updated_at.using(Node.node_updated_at).apply(query)

        return query


class NodeTypeFilter(Filter, Generic[N]):
    """Filter nodes by their type."""

    any_of: Sequence[type[N]] = ()
    """Nodes must be one of these types."""

    none_of: Sequence[type[N]] = ()
    """Nodes must not be one of these types."""

    def apply(self, query: Select) -> Select:
        if self.any_of:
            query = query.where(
                Node.node_type.in_(
                    {c.polymorphic_identity for c in self.any_of}
                    | {s.polymorphic_identity for c in self.any_of for s in c.__subclasses__()}
                )
            )

        if self.none_of:
            query = query.where(
                Node.node_type.notin_(
                    {c.polymorphic_identity for c in self.none_of}
                    | {s.polymorphic_identity for c in self.none_of for s in c.__subclasses__()}
                )
            )

        return query


class ArtifactFilter(NodeFilter[A]):
    """Filter artifacts that meet the given conditions."""

    node_type: NodeTypeFilter[A] = field(
        # delay this in case tables are defined late
        default_factory=lambda: NodeTypeFilter(any_of=[BaseArtifact])  # type: ignore
    )
    """Artifacts must be one of these types."""

    artifact_label: ComparisonFilter[str] | None = None
    """Filter artifacts by their label."""

    artifact_serializer: Sequence[Serializer] = ()
    """Filter artifacts by their serializer."""

    def apply(self, query: Select) -> Select:
        query = super().apply(query)

        if self.artifact_label:
            query = self.artifact_label.using(BaseArtifact.artifact_label).apply(query)

        if self.artifact_serializer:
            query = query.where(
                BaseArtifact.artifact_serializer.in_(s.name for s in self.artifact_serializer)
            )

        return query


def column_op(
    *,
    op: Callable[[InstrumentedAttribute[Any], Any], BinaryExpression],
    **kwargs: Any,
) -> Any:
    """Apply a comparison operator to a column."""
    return field(metadata={"op": op}, **kwargs)


@dataclass_transform(field_specifiers=())
class ComparisonFilter(GenericFilter[InstrumentedAttribute[T]]):
    """Filter a column by comparing it to a value."""

    gt: T | None = column_op(default=None, op=operator.gt)
    """The column must be greater than this value."""
    ge: T | None = column_op(default=None, op=operator.ge)
    """The column must be greater than or equal to this value."""
    lt: T | None = column_op(default=None, op=operator.lt)
    """The column must be less than this value."""
    le: T | None = column_op(default=None, op=operator.le)
    """The column must be less than or equal to this value."""
    eq: T | None = column_op(default=None, op=operator.eq)
    """The column must be equal to this value."""
    in_: Sequence[T] | None = column_op(default=None, op=lambda col, val: col.in_(val))
    """The column must be one of these values."""
    not_in: Sequence[T] | None = column_op(default=None, op=lambda col, val: col.notin_(val))
    """The column must not be one of these values."""
    like: T | None = column_op(default=None, op=lambda col, val: col.like(val))
    """The column must match this pattern."""
    ilike: T | None = column_op(default=None, op=lambda col, val: col.ilike(val))
    """The column must match this pattern, case-insensitive."""
    is_: bool | None = column_op(default=None, op=lambda col, val: col.is_(val))
    """The column must be this value."""
    is_not: bool | None = column_op(default=None, op=lambda col, val: col.isnot(val))
    """The column must not be this value."""

    def apply(self, query: Select) -> Select:
        for f in fields(self):
            if "op" in f.metadata:
                op_value = getattr(self, f.name, None)
                if op_value is not None:
                    op: Callable[[InstrumentedAttribute[T], T], BinaryExpression] = f.metadata["op"]
                    query = query.where(op(self.value, op_value))
        return query
