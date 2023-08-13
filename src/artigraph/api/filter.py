from __future__ import annotations

import operator
import sys
from dataclasses import dataclass, field, fields, replace
from datetime import datetime
from typing import TYPE_CHECKING, Any, Callable, Collection, Generic, Sequence, TypeVar

from sqlalchemy import BinaryExpression, Column, select
from sqlalchemy.orm import aliased
from sqlalchemy.orm.attributes import InstrumentedAttribute
from sqlalchemy.sql.dml import Delete, Update
from sqlalchemy.sql.selectable import Select
from typing_extensions import ParamSpec, Self, dataclass_transform

from artigraph.orm import BaseArtifact, Node, get_polymorphic_identities

P = ParamSpec("P")
T = TypeVar("T")
N = TypeVar("N", bound=Node)
A = TypeVar("A", bound=BaseArtifact)
Query = TypeVar("Query", bound="Select[Any] | Update | Delete")


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
        kwargs = kwargs if sys.version_info < (3, 10) else {"kw_only": True, **kwargs}
        self = dataclass(**kwargs)(self)
        return self


class Filter(metaclass=_FilterMeta):
    """Base class for where clauses."""

    def apply(self, query: Query, /) -> Query:
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
        value: T = field(init=False, repr=False)

    def using(self, value: T) -> Self:
        """Return a new filter that uses the given value."""
        new = replace(self)
        new.value = value  # type: ignore
        return new

    def apply(self, query: Query) -> Query:
        raise NotImplementedError()


class NodeFilter(Filter, Generic[N]):
    """Filter nodes that meet the given conditions"""

    node_id: ValueFilter[int] | int | None = None
    """Nodes must have this ID or meet this condition."""
    node_type: NodeTypeFilter[N] | type[N] | None = None
    """Nodes must be one of these types."""
    relationship: NodeRelationshipFilter | None = None
    """Nodes must be related to one of these nodes."""
    created_at: ValueFilter[datetime] | datetime | None = None
    """Filter nodes by their creation time."""
    updated_at: ValueFilter[datetime] | datetime | None = None
    """Filter nodes by their last update time."""

    def apply(self, query: Query) -> Query:
        if self.node_id is not None:
            query = to_value_filter(self.node_id).using(Node.node_id).apply(query)

        if self.node_type is not None:
            query = (
                self.node_type
                if isinstance(self.node_type, NodeTypeFilter)
                else NodeTypeFilter(type=[self.node_type])
            ).apply(query)

        if self.relationship:
            query = self.relationship.apply(query)

        if self.created_at:
            query = to_value_filter(self.created_at).using(Node.node_created_at).apply(query)

        if self.updated_at:
            query = to_value_filter(self.updated_at).using(Node.node_updated_at).apply(query)

        return query


class NodeRelationshipFilter(Filter):
    """Filter nodes by their relationships to other nodes."""

    include_self: bool = False
    """Include the given nodes in the results."""
    child_of: Sequence[int] | int | None = None
    """Nodes must be a child of one of these nodes."""
    parent_of: Sequence[int] | int | None = None
    """Nodes must be a parent of one of these nodes."""
    descendant_of: Sequence[int] | int | None = None
    """Nodes must be a descendant of one of these nodes."""
    ancestor_of: Sequence[int] | int | None = None
    """Nodes must be an ancestor of one of these nodes."""

    def apply(self, query: Query) -> Query:
        child_of = to_sequence_or_none(self.child_of)
        parent_of = to_sequence_or_none(self.parent_of)
        descendant_of = to_sequence_or_none(self.descendant_of)
        ancestor_of = to_sequence_or_none(self.ancestor_of)

        if not self.include_self:
            query = query.where(
                Node.node_id.notin_(
                    [
                        *(child_of or []),
                        *(parent_of or []),
                        *(descendant_of or []),
                        *(ancestor_of or []),
                    ]
                )
            )

        if child_of is not None:
            query = query.where(Node.node_parent_id.in_(n for n in child_of))

        if parent_of is not None:
            query = query.where(
                Node.node_id.in_(
                    select(Node.node_parent_id).where(Node.node_id.in_(n for n in parent_of))
                )
            )

        if descendant_of is not None:
            descendant_node_cte = (
                select(Node.node_id.label("descendant_id"), Node.node_parent_id)
                .where(Node.node_id.in_(descendant_of))
                .cte(name="descendants", recursive=True)
            )

            # Recursive case: select the children of the current nodes
            parent_node = aliased(Node)
            descendant_node_cte = descendant_node_cte.union_all(
                select(parent_node.node_id, parent_node.node_parent_id).where(
                    parent_node.node_parent_id == descendant_node_cte.c.descendant_id
                )
            )

            query = query.where(
                Node.node_id.in_(
                    select(descendant_node_cte.c.descendant_id).where(
                        descendant_node_cte.c.descendant_id.isnot(None)
                    )
                )
            )

        if ancestor_of is not None:
            # Create a CTE to get the ancestors recursively
            ancestor_node_cte = (
                select(Node.node_id.label("ancestor_id"), Node.node_parent_id)
                .where(Node.node_id.in_(ancestor_of))
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
            query = query.where(
                Node.node_id.in_(
                    select(ancestor_node_cte.c.ancestor_id).where(
                        ancestor_node_cte.c.ancestor_id.isnot(None)
                    )
                )
            )

        return query


class NodeTypeFilter(Filter, Generic[N]):
    """Filter nodes by their type."""

    subclasses: bool = True
    """Consider subclasses of the given types when filtering."""
    type: Sequence[type[N]] | type[N] | None = None  # noqa: A003
    """Nodes must be one of these types."""
    not_type: Sequence[type[N]] | type[N] | None = None
    """Nodes must not be one of these types."""

    def apply(self, query: Query) -> Query:
        type_in = to_sequence_or_none(self.type)
        type_not_in = to_sequence_or_none(self.not_type)

        if type_in is not None:
            polys_in = get_polymorphic_identities(type_in, subclasses=self.subclasses)
            query = query.where(Node.node_type.in_(polys_in))

        if type_not_in is not None:
            polys_not_in = get_polymorphic_identities(type_not_in, subclasses=self.subclasses)
            query = query.where(Node.node_type.notin_(polys_not_in))

        return query


class ArtifactFilter(NodeFilter[A]):
    """Filter artifacts that meet the given conditions."""

    node_type: NodeTypeFilter[A] = field(
        # delay this in case tables are defined late
        default_factory=lambda: NodeTypeFilter(type=[BaseArtifact])  # type: ignore
    )
    """Artifacts must be one of these types."""
    artifact_label: ValueFilter[str] | str | None = None
    """Filter artifacts by their label."""

    def apply(self, query: Query) -> Query:
        query = super().apply(query)

        if self.artifact_label:
            query = (
                to_value_filter(self.artifact_label).using(BaseArtifact.artifact_label).apply(query)
            )

        return query


def column_op(*, op: Callable[[Any, Any], BinaryExpression], **kwargs: Any) -> Any:
    """Apply a comparison operator to a column."""
    return field(metadata={"op": op}, **kwargs)


@dataclass_transform(field_specifiers=())
class ValueFilter(GenericFilter[InstrumentedAttribute[T]]):
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
    in_: Collection[T] | None = column_op(default=None, op=Column.in_)
    """The column must be one of these values."""
    not_in: Collection[T] | None = column_op(default=None, op=Column.notin_)
    """The column must not be one of these values."""
    like: T | None = column_op(default=None, op=Column.like)
    """The column must match this pattern."""
    ilike: T | None = column_op(default=None, op=Column.ilike)
    """The column must match this pattern, case-insensitive."""
    is_: bool | None = column_op(default=None, op=Column.is_)
    """The column must be this value."""
    is_not: bool | None = column_op(default=None, op=Column.isnot)
    """The column must not be this value."""

    def apply(self, query: Query) -> Query:
        for f in fields(self):
            if "op" in f.metadata:
                op_value = getattr(self, f.name, None)
                if op_value is not None:
                    op: Callable[[InstrumentedAttribute[T], T], BinaryExpression] = f.metadata["op"]
                    query = query.where(op(self.value, op_value))
        return query


def to_sequence_or_none(value: Sequence[T] | T | None) -> Sequence[T] | None:
    """Convert scalar values to a sequence, or None if the value is None."""
    return value if isinstance(value, Sequence) else (None if value is None else (value,))


def to_value_filter(value: T | ValueFilter[T]) -> ValueFilter[T]:
    """If not a `ValueFilter`, cast to one that checks for equivalence."""
    return value if isinstance(value, ValueFilter) else ValueFilter(eq=value)
