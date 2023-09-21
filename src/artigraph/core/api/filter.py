from __future__ import annotations

import operator
from dataclasses import field, fields, replace
from datetime import datetime
from typing import (
    Any,
    Callable,
    Collection,
    Generic,
    Literal,
    Sequence,
    TypeVar,
    cast,
)
from uuid import UUID

from sqlalchemy import (
    BinaryExpression,
    Column,
    ColumnElement,
    Delete,
    Exists,
    Select,
    Update,
    select,
)
from sqlalchemy.orm import aliased
from sqlalchemy.orm.attributes import InstrumentedAttribute
from sqlalchemy.sql.elements import ExpressionClauseList
from sqlalchemy.sql.operators import OperatorType
from typing_extensions import ParamSpec, Self

from artigraph.core.orm.artifact import OrmArtifact
from artigraph.core.orm.link import OrmLink
from artigraph.core.orm.node import OrmNode, get_polymorphic_identities
from artigraph.core.utils.misc import FrozenDataclass

P = ParamSpec("P")
T = TypeVar("T")
N = TypeVar("N", bound=OrmNode)
A = TypeVar("A", bound=OrmArtifact)
Q = TypeVar("Q", bound="Select | Update | Delete | Exists")

Expression = ColumnElement[bool]
"""An alias for a sqlalchemy `OperatorExpression`"""


# An empty filter that does nothing
_NO_OP = ExpressionClauseList(cast(OperatorType, operator.and_))


class Filter(FrozenDataclass):
    """Base class for where clauses."""

    def create(self) -> Expression:
        """Return the condition represented by this filter."""
        return self.compose(_NO_OP)

    def compose(self, expr: Expression, /) -> Expression:
        """Apply the where clause to the given query."""
        raise NotImplementedError()

    def __and__(self, other: Filter) -> MultiFilter:
        """Combine this filter with another."""
        return MultiFilter(op="and", filters=(self, other))

    def __or__(self, other: Filter) -> MultiFilter:
        """Combine this filter with another."""
        return MultiFilter(op="or", filters=(self, other))

    def __str__(self) -> str:
        return str(self.create().compile(compile_kwargs={"literal_binds": True}))


class MultiFilter(Filter):
    """A filter that applies multiple filters."""

    op: Literal["and", "or"]
    """The operator to use when combining filters."""

    filters: Sequence[Filter]
    """The filters to apply."""

    def compose(self, expr: Expression) -> Expression:
        if self.op == "and":
            for f in self.filters:
                expr &= f.create()
        else:
            for f in self.filters:
                expr |= f.create()
        return expr

    def __and__(self, other: Filter) -> MultiFilter:
        """Combine this filter with another."""
        return self._combine("and", other)

    def __or__(self, other: Filter) -> MultiFilter:
        """Combine this filter with another."""
        return self._combine("or", other)

    def _combine(self, new_op: Literal["or", "and"], other: Filter) -> MultiFilter:
        if self.op == new_op:
            if isinstance(other, MultiFilter) and other.op == self.op:
                return MultiFilter(op=self.op, filters=(*self.filters, *other.filters))
            else:
                return MultiFilter(op=self.op, filters=(*self.filters, other))
        else:
            return MultiFilter(op=new_op, filters=(self, other))


class NodeFilter(Filter, Generic[N]):
    """Filter nodes that meet the given conditions"""

    id: ValueFilter[UUID] | Sequence[UUID] | UUID | None = None
    """Nodes must have this ID or meet this condition."""
    node_type: NodeTypeFilter[N] | type[N] | None = None
    """Nodes must be one of these types."""
    created_at: ValueFilter[datetime] | datetime | None = None
    """Filter nodes by their creation time."""
    updated_at: ValueFilter[datetime] | datetime | None = None
    """Filter nodes by their last update time."""
    parent_of: NodeFilter | Sequence[UUID] | UUID | None = None
    """Nodes must be the parent of one of these nodes."""
    child_of: NodeFilter | Sequence[UUID] | UUID | None = None
    """Nodes must be the child of one of these nodes."""
    descendant_of: NodeFilter | Sequence[UUID] | UUID | None = None
    """Nodes must be the descendant of one of these nodes."""
    ancestor_of: NodeFilter | Sequence[UUID] | UUID | None = None
    """Nodes must be the ancestor of one of these nodes."""
    label: ValueFilter[str] | Sequence[str] | str | None = None
    """Nodes must have a link with one of these labels."""

    def compose(self, expr: Expression) -> Expression:
        node_id = to_value_filter(self.id)
        created_at = to_value_filter(self.created_at)
        updated_at = to_value_filter(self.updated_at)

        if node_id is not None:
            expr &= node_id.against(OrmNode.id).create()

        if self.node_type is not None:
            expr &= (
                self.node_type
                if isinstance(self.node_type, NodeTypeFilter)
                else NodeTypeFilter(type=[self.node_type])
            ).create()

        if created_at:
            expr &= created_at.against(OrmNode.created_at).create()

        if updated_at:
            expr &= updated_at.against(OrmNode.updated_at).create()

        if self.parent_of or self.ancestor_of:
            expr &= OrmNode.id.in_(
                select(OrmLink.source_id).where(
                    LinkFilter(child=self.parent_of, descendant=self.ancestor_of).create()
                )
            )

        if self.child_of or self.descendant_of:
            expr &= OrmNode.id.in_(
                select(OrmLink.target_id).where(
                    LinkFilter(parent=self.child_of, ancestor=self.descendant_of).create()
                )
            )

        if self.label:
            expr &= OrmNode.id.in_(
                select(OrmLink.target_id).where(LinkFilter(label=self.label).create())
            )

        return expr


class LinkFilter(Filter):
    """Filter node links."""

    id: ValueFilter[UUID] | UUID | None = None
    """Links must have this ID or meet this condition."""
    parent: NodeFilter | Sequence[UUID] | UUID | None = None
    """Links must have one of these nodes as their parent."""
    child: NodeFilter | Sequence[UUID] | UUID | None = None
    """Links must have one of these nodes as their child."""
    descendant: NodeFilter | Sequence[UUID] | UUID | None = None
    """Links must have one of these nodes as their descendant."""
    ancestor: NodeFilter | Sequence[UUID] | UUID | None = None
    """Links must have one of these nodes as their ancestor."""
    label: ValueFilter[str] | Sequence[str] | str | None = None

    def compose(self, expr: Expression) -> Expression:
        link_id = to_value_filter(self.id)
        source_id = to_node_id_selector(self.parent)
        target_id = to_node_id_selector(self.child)
        descendant_id = to_node_id_selector(self.descendant)
        ancestor_id = to_node_id_selector(self.ancestor)
        label = to_value_filter(self.label)

        if link_id is not None:
            expr &= link_id.against(OrmLink.id).create()

        if source_id is not None:
            expr &= OrmLink.source_id.in_(source_id)

        if target_id is not None:
            expr &= OrmLink.target_id.in_(target_id)

        if ancestor_id is not None:
            # Create a CTE to get the descendants recursively
            descendant_node_cte = (
                select(OrmLink.source_id.label("descendant_id"), OrmLink.target_id)
                .where(OrmLink.source_id.in_(ancestor_id))
                .cte(name="descendants", recursive=True)
            )

            # Recursive case: select the children of the current nodes
            child_node = aliased(OrmLink)
            descendant_node_cte = descendant_node_cte.union_all(
                select(child_node.source_id, child_node.target_id).where(
                    child_node.source_id == descendant_node_cte.c.target_id
                )
            )

            # Join the CTE with the actual Node table to get the descendants
            expr &= OrmLink.source_id.in_(
                select(descendant_node_cte.c.descendant_id).where(
                    descendant_node_cte.c.descendant_id.isnot(None)
                )
            )

        if descendant_id is not None:
            # Create a CTE to get the ancestors recursively
            ancestor_node_cte = (
                select(OrmLink.target_id.label("ancestor_id"), OrmLink.source_id)
                .where(OrmLink.target_id.in_(descendant_id))
                .cte(name="ancestors", recursive=True)
            )

            # Recursive case: select the parents of the current nodes
            parent_node = aliased(OrmLink)
            ancestor_node_cte = ancestor_node_cte.union_all(
                select(parent_node.target_id, parent_node.source_id).where(
                    parent_node.target_id == ancestor_node_cte.c.source_id
                )
            )

            # Join the CTE with the actual Node table to get the ancestors
            expr &= OrmLink.target_id.in_(
                select(ancestor_node_cte.c.ancestor_id).where(
                    ancestor_node_cte.c.ancestor_id.isnot(None)
                )
            )

        if label is not None:
            expr &= label.against(OrmLink.label).create()

        return expr


class NodeTypeFilter(Filter, Generic[N]):
    """Filter nodes by their type."""

    subclasses: bool = True
    """Consider subclasses of the given types when filtering."""
    type: Sequence[type[N]] | type[N] | None = None
    """Nodes must be one of these types."""
    not_type: Sequence[type[N]] | type[N] | None = None
    """Nodes must not be one of these types."""

    def compose(self, expr: Expression) -> Expression:
        type_in = to_sequence_or_none(self.type)
        type_not_in = to_sequence_or_none(self.not_type)

        if type_in is not None:
            polys_in = get_polymorphic_identities(type_in, subclasses=self.subclasses)
            expr &= OrmNode.node_type.in_(polys_in)

        if type_not_in is not None:
            polys_not_in = get_polymorphic_identities(type_not_in, subclasses=self.subclasses)
            expr &= OrmNode.node_type.notin_(polys_not_in)

        return expr


class ArtifactFilter(NodeFilter[A]):
    """Filter artifacts that meet the given conditions."""

    node_type: NodeTypeFilter[A] = field(
        # delay this in case tables are defined late
        default_factory=lambda: NodeTypeFilter(type=[OrmArtifact])  # type: ignore
    )
    """Artifacts must be one of these types."""


def column_op(*, op: Callable[[Any, Any], BinaryExpression], **kwargs: Any) -> Any:
    """Apply a comparison operator to a column."""
    return field(metadata={"op": op}, **kwargs)


class ValueFilter(Filter, Generic[T]):
    """Filter a column by comparing it to a value."""

    column: InstrumentedAttribute[T] | None = field(repr=False, default=None)
    """The column to filter."""

    gt: T | None = column_op(default=None, op=operator.gt)
    """The column must be greater than this value."""
    ge: T | None = column_op(default=None, op=operator.ge)
    """The column must be greater than or equal to this value."""
    lt: T | None = column_op(default=None, op=operator.lt)
    """The column must be less than this value."""
    le: T | None = column_op(default=None, op=operator.le)
    """The column must be less than or equal to this value."""
    ne: T | None = column_op(default=None, op=operator.ne)
    """The column must not be equal to this value."""
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

    def against(self, column: InstrumentedAttribute[T] | InstrumentedAttribute[T | None]) -> Self:
        """Filter against the given column."""
        return replace(self, column=column)

    def compose(self, expr: Expression) -> Expression:
        # InstrumentedAttribute is a descriptor so the type checker thinks
        # self.column is of type T, not InstrumentedAttribute[T]
        column = cast(InstrumentedAttribute[T] | None, self.column)

        if column is None:  # nocov
            msg = "No column to filter against - did you forget to call `against`?"
            raise ValueError(msg)

        for f in fields(self):
            if "op" in f.metadata:
                op_value = getattr(self, f.name, None)
                if op_value is not None:
                    op: Callable[[InstrumentedAttribute[T], T], BinaryExpression] = f.metadata["op"]
                    expr &= op(column, op_value)

        return expr


def to_sequence_or_none(value: Sequence[T] | T | None) -> Sequence[T] | None:
    """Convert scalar values to a sequence, or None if the value is None."""
    return value if isinstance(value, Sequence) else (None if value is None else (value,))


def to_value_filter(value: T | Sequence[T] | ValueFilter[T] | None) -> ValueFilter[T] | None:
    """If not a `ValueFilter`, cast to one that checks for equivalence."""
    if value is None or isinstance(value, ValueFilter):
        return value

    if isinstance(value, str):
        return ValueFilter(eq=value)

    if isinstance(value, Sequence):
        return ValueFilter(in_=value)

    return ValueFilter(eq=value)


def to_node_id_selector(
    value: NodeFilter | Sequence[UUID] | UUID | None,
) -> Select[tuple[UUID, ...]] | Sequence[UUID] | None:
    """Convert a node filter to a node ID selector."""
    if value is None:
        return value

    if isinstance(value, Filter):
        return select(OrmNode.id).where(value.create())

    if isinstance(value, UUID):
        return [value]

    return value
