from __future__ import annotations

import operator
import sys
from dataclasses import dataclass, field, fields, replace
from datetime import datetime
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Collection,
    Generic,
    Literal,
    Sequence,
    TypeVar,
)

from sqlalchemy import (
    BinaryExpression,
    BooleanClauseList,
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
from typing_extensions import ParamSpec, Self, dataclass_transform

from artigraph.orm import BaseArtifact, Node, get_polymorphic_identities
from artigraph.orm.link import NodeLink

P = ParamSpec("P")
T = TypeVar("T")
N = TypeVar("N", bound=Node)
A = TypeVar("A", bound=BaseArtifact)
Q = TypeVar("Q", bound="Select | Update | Delete | Exists")

Expression = ColumnElement[bool]
"""An alias for a sqlalchemy `OperatorExpression`"""


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


# An empty filter that does nothing
_NO_OP = BooleanClauseList.and_()


class Filter(metaclass=_FilterMeta):
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


class GenericFilter(Filter, Generic[T]):
    """Base class for filters that apply generally to a given value."""

    if TYPE_CHECKING:
        # type checkers assume T.__get__ if T is a descriptor - force it to just be T

        @property
        def value(self) -> T:  # type: ignore
            """The specific value to filter by."""

        @value.setter
        def value(self, new: T) -> None:  # type: ignore
            """Set the value to filter by."""

    else:
        value: T = field(init=False, repr=False)

    def use(self, value: T) -> Self:
        """Return a new filter that uses the given value."""
        new = replace(self)
        new.value = value
        return new


class NodeFilter(Filter, Generic[N]):
    """Filter nodes that meet the given conditions"""

    node_id: ValueFilter[str] | Sequence[str] | str | None = None
    """Nodes must have this ID or meet this condition."""
    node_type: NodeTypeFilter[N] | type[N] | None = None
    """Nodes must be one of these types."""
    created_at: ValueFilter[datetime] | datetime | None = None
    """Filter nodes by their creation time."""
    updated_at: ValueFilter[datetime] | datetime | None = None
    """Filter nodes by their last update time."""
    parent_of: NodeFilter | Sequence[str] | str | None = None
    """Nodes must be the parent of one of these nodes."""
    child_of: NodeFilter | Sequence[str] | str | None = None
    """Nodes must be the child of one of these nodes."""
    descendant_of: NodeFilter | Sequence[str] | str | None = None
    """Nodes must be the descendant of one of these nodes."""
    ancestor_of: NodeFilter | Sequence[str] | str | None = None
    """Nodes must be the ancestor of one of these nodes."""

    def compose(self, expr: Expression) -> Expression:
        node_id = to_value_filter(self.node_id)

        if node_id is not None:
            expr &= node_id.use(Node.node_id).create()

        if self.node_type is not None:
            expr &= (
                self.node_type
                if isinstance(self.node_type, NodeTypeFilter)
                else NodeTypeFilter(type=[self.node_type])
            ).create()

        if self.created_at:
            expr &= to_value_filter(self.created_at).use(Node.created_at).create()

        if self.updated_at:
            expr &= to_value_filter(self.updated_at).use(Node.updated_at).create()

        if self.parent_of or self.ancestor_of:
            expr &= Node.node_id.in_(
                select(NodeLink.parent_id).where(
                    NodeLinkFilter(child=self.parent_of, descendant=self.ancestor_of).create()
                )
            )

        if self.child_of or self.descendant_of:
            expr &= Node.node_id.in_(
                select(NodeLink.child_id).where(
                    NodeLinkFilter(parent=self.child_of, ancestor=self.descendant_of).create()
                )
            )

        return expr


class NodeLinkFilter(Filter):
    """Filter node links."""

    parent: NodeFilter | Sequence[str] | str | None = None
    child: NodeFilter | Sequence[str] | str | None = None
    descendant: NodeFilter | Sequence[str] | str | None = None
    ancestor: NodeFilter | Sequence[str] | str | None = None

    def compose(self, expr: Expression) -> Expression:
        parent_id = to_node_id_selector(self.parent)
        child_id = to_node_id_selector(self.child)
        descendant_id = to_node_id_selector(self.descendant)
        ancestor_id = to_node_id_selector(self.ancestor)

        if parent_id is not None:
            expr &= NodeLink.parent_id.in_(parent_id)

        if child_id is not None:
            expr &= NodeLink.child_id.in_(child_id)

        if ancestor_id is not None:
            # Create a CTE to get the descendants recursively
            descendant_node_cte = (
                select(NodeLink.parent_id.label("descendant_id"), NodeLink.child_id)
                .where(NodeLink.parent_id.in_(ancestor_id))
                .cte(name="descendants", recursive=True)
            )

            # Recursive case: select the children of the current nodes
            child_node = aliased(NodeLink)
            descendant_node_cte = descendant_node_cte.union_all(
                select(child_node.parent_id, child_node.child_id).where(
                    child_node.parent_id == descendant_node_cte.c.child_id
                )
            )

            # Join the CTE with the actual Node table to get the descendants
            expr &= NodeLink.parent_id.in_(
                select(descendant_node_cte.c.descendant_id).where(
                    descendant_node_cte.c.descendant_id.isnot(None)
                )
            )

        if descendant_id is not None:
            # Create a CTE to get the ancestors recursively
            ancestor_node_cte = (
                select(NodeLink.child_id.label("ancestor_id"), NodeLink.parent_id)
                .where(NodeLink.child_id.in_(descendant_id))
                .cte(name="ancestors", recursive=True)
            )

            # Recursive case: select the parents of the current nodes
            parent_node = aliased(NodeLink)
            ancestor_node_cte = ancestor_node_cte.union_all(
                select(parent_node.child_id, parent_node.parent_id).where(
                    parent_node.child_id == ancestor_node_cte.c.parent_id
                )
            )

            # Join the CTE with the actual Node table to get the ancestors
            expr &= NodeLink.child_id.in_(
                select(ancestor_node_cte.c.ancestor_id).where(
                    ancestor_node_cte.c.ancestor_id.isnot(None)
                )
            )

        return expr


class NodeTypeFilter(Filter, Generic[N]):
    """Filter nodes by their type."""

    subclasses: bool = True
    """Consider subclasses of the given types when filtering."""
    type: Sequence[type[N]] | type[N] | None = None  # noqa: A003
    """Nodes must be one of these types."""
    not_type: Sequence[type[N]] | type[N] | None = None
    """Nodes must not be one of these types."""

    def compose(self, expr: Expression) -> Expression:
        type_in = to_sequence_or_none(self.type)
        type_not_in = to_sequence_or_none(self.not_type)

        if type_in is not None:
            polys_in = get_polymorphic_identities(type_in, subclasses=self.subclasses)
            expr &= Node.node_type.in_(polys_in)

        if type_not_in is not None:
            polys_not_in = get_polymorphic_identities(type_not_in, subclasses=self.subclasses)
            expr &= Node.node_type.notin_(polys_not_in)

        return expr


class ArtifactFilter(NodeFilter[A]):
    """Filter artifacts that meet the given conditions."""

    node_type: NodeTypeFilter[A] = field(
        # delay this in case tables are defined late
        default_factory=lambda: NodeTypeFilter(type=[BaseArtifact])  # type: ignore
    )
    """Artifacts must be one of these types."""
    artifact_label: ValueFilter[str] | str | None = None
    """Filter artifacts by their label."""

    def compose(self, expr: Expression) -> Expression:
        expr = super().compose(expr)

        if self.artifact_label:
            expr = (
                to_value_filter(self.artifact_label).use(BaseArtifact.artifact_label).compose(expr)
            )

        return expr


def column_op(*, op: Callable[[Any, Any], BinaryExpression], **kwargs: Any) -> Any:
    """Apply a comparison operator to a column."""
    return field(metadata={"op": op}, **kwargs)


@dataclass_transform()
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

    def compose(self, expr: Expression) -> Expression:
        for f in fields(self):
            if "op" in f.metadata:
                op_value = getattr(self, f.name, None)
                if op_value is not None:
                    op: Callable[[InstrumentedAttribute[T], T], BinaryExpression] = f.metadata["op"]
                    expr &= op(self.value, op_value)
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
    value: NodeFilter | Sequence[str] | str | None,
) -> Select[str] | Sequence[str] | None:
    """Convert a node filter to a node ID selector."""
    if value is None:
        return value

    if isinstance(value, NodeFilter):
        return select(Node.node_id).where(value.create())

    if isinstance(value, str):
        return [value]

    return value
