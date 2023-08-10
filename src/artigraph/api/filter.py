from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import datetime
from typing import TYPE_CHECKING, Any, Generic, Sequence, TypeVar

from sqlalchemy import join, select
from sqlalchemy.orm import aliased
from sqlalchemy.orm.attributes import InstrumentedAttribute
from sqlalchemy.sql.selectable import Select
from typing_extensions import ParamSpec, Self, dataclass_transform

from artigraph.orm import BaseArtifact, Node

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
        self = dataclass(frozen=True, **kwargs)(self)
        return self


class Filter(metaclass=_FilterMeta):
    """Base class for where clauses."""

    def apply(self, query: Select, /) -> Select:
        """Apply the where clause to the given query."""
        raise NotImplementedError()


class GenericFilter(Generic[T], Filter):
    """Base class for filters that apply generally to a given value."""

    if TYPE_CHECKING:
        # type checkers assume T.__get__ if T is a descriptor - force it to just be T

        @property
        def value(self) -> T:
            """The specific value to filter by."""

    else:
        value = field(init=False)

    def using(self, value: T) -> Self:
        """Return a new filter that uses the given value."""
        new = replace(self)
        object.__setattr__(self, "value", value)
        return new

    def apply(self, query: Select) -> Select:
        raise NotImplementedError()


class NodeFilter(Generic[N], Filter):
    """Filter nodes that meet the given conditions"""

    in_ids: Sequence[int] = ()
    """Nodes must have one of these IDs."""

    in_types: Sequence[type[N]] = ()
    """Nodes must be one of these types."""

    not_in_types: Sequence[type[Node]] = ()
    """Nodes must not be one of these types."""

    is_child_of: Sequence[Node] = ()
    """Nodes must be a child of one of these nodes."""

    is_parent_of: Sequence[Node] = ()
    """Nodes must be a parent to any one of these nodes."""

    is_descendant_of: Sequence[Node] = ()
    """Nodes must be a descendant of one of these nodes."""

    is_ancestor_of: Sequence[Node] = ()
    """Nodes must be an ancestor of one of these nodes."""

    created_at: DatetimeFilter | None
    """Filter nodes by their creation time."""

    updated_at: DatetimeFilter | None
    """Filter nodes by their last update time."""

    def apply(self, query: Select) -> Select:
        if self.in_ids:
            query = query.where(Node.node_id.in_(self.in_ids))

        if self.in_types:
            query = query.where(Node.node_type.in_(n.polymorphic_identity for n in self.in_types))

        if self.not_in_types:
            query = query.where(
                Node.node_type.notin_(n.polymorphic_identity for n in self.not_in_types)
            )

        if self.is_child_of:
            query = query.where(Node.node_parent_id.in_(n.node_id for n in self.is_child_of))

        if self.is_parent_of:
            query = query.where(Node.node_id.in_(n.node_parent_id for n in self.is_parent_of))

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
            ancestor_node_cte = (
                select(Node.node_id.label("ancestor_id"), Node.node_parent_id)
                .where(Node.node_id.in_(self.is_ancestor_of))
                .cte(name="ancestors", recursive=True)
            )

            # Recursive case: select the parents of the current nodes
            child_node = aliased(Node)
            ancestor_node_cte = ancestor_node_cte.union_all(
                select(child_node.node_id, child_node.node_parent_id).where(
                    child_node.node_id == ancestor_node_cte.c.ancestor_id
                )
            )

            # Join the CTE with the actual Node table to get the ancestors
            query = (
                query.select_from(
                    join(
                        Node,
                        ancestor_node_cte,
                        Node.node_id == ancestor_node_cte.c.ancestor_id,
                    )
                )
                # Exclude the roots
                .where(ancestor_node_cte.c.ancestor_id.notin_(self.is_ancestor_of))
            )

        if self.created_at:
            query = self.created_at.using(Node.node_created_at).apply(query)

        if self.updated_at:
            query = self.updated_at.using(Node.node_updated_at).apply(query)

        return query


class DatetimeFilter(GenericFilter[InstrumentedAttribute[datetime]]):
    """Filter a datetime column by a range of datetimes."""

    gt: datetime | None = None
    """The minimum datetime."""

    ge: datetime | None = None
    """The minimum datetime, inclusive."""

    lt: datetime | None = None
    """The maximum datetime."""

    le: datetime | None = None
    """The maximum datetime, inclusive."""

    eq: datetime | None = None
    """The exact datetime."""

    def apply(self, query: Select) -> Select:
        if self.gt:
            query = query.where(self.value > self.gt)
        if self.ge:
            query = query.where(self.value >= self.ge)
        if self.lt:
            query = query.where(self.value < self.lt)
        if self.le:
            query = query.where(self.value <= self.le)
        if self.eq:
            query = query.where(self.value == self.eq)
        return query
