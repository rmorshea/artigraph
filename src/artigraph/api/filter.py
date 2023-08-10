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
        def value(self) -> T:
            """The specific value to filter by."""

    else:
        value = field(init=False)

    def using(self, value: T) -> Self:
        """Return a new filter that uses the given value."""
        new = replace(self)
        new.value = value
        return new

    def apply(self, query: Select) -> Select:
        raise NotImplementedError()


class NodeFilter(Filter, Generic[N]):
    """Filter nodes that meet the given conditions"""

    node_id: IntegerFilter | None
    """Nodes must have one of these IDs."""

    node_type: NodeTypeFilter[N] | None = None
    """Nodes must be one of these types."""

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
        if self.node_id:
            query = self.node_id.apply(query)

        if self.node_type:
            query = self.node_type.apply(query)

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


class NodeTypeFilter(Filter, Generic[N]):
    """Filter nodes by their type."""

    any_of: Sequence[type[N]] = ()
    """Nodes must be one of these types."""

    none_of: Sequence[type[N]] = ()
    """Nodes must not be one of these types."""

    def apply(self, query: Select) -> Select:
        if self.any_of:
            query = query.where(Node.node_type.in_(n.polymorphic_identity for n in self.any_of))

        if self.none_of:
            query = query.where(Node.node_type.notin_(n.polymorphic_identity for n in self.none_of))

        return query


class ArtifactFilter(NodeFilter[BaseArtifact]):
    """Filter artifacts that meet the given conditions."""

    node_type: NodeTypeFilter = field(init=False)

    artifact_label: StringFilter | None = None
    """Filter artifacts by their label."""

    artifact_detail: StringFilter | None = None
    """Filter artifacts by their detail."""

    artifact_serializer: Sequence[Serializer] = ()
    """Filter artifacts by their serializer."""

    def apply(self, query: Select) -> Select:
        query = super().apply(query)

        self.node_type = NodeTypeFilter(any_of=BaseArtifact.__subclasses__())

        if self.artifact_label:
            query = self.artifact_label.using(BaseArtifact.artifact_label).apply(query)

        if self.artifact_detail:
            query = self.artifact_detail.using(BaseArtifact.artifact_detail).apply(query)

        if self.artifact_serializer:
            query = query.where(
                BaseArtifact.artifact_serializer.in_(s.name for s in self.artifact_serializer)
            )

        return query


class IntegerFilter(GenericFilter[InstrumentedAttribute[int]]):
    """Filter an integer column by a range of integers."""

    gt: int | None = None
    """The minimum integer."""

    ge: int | None = None
    """The minimum integer, inclusive."""

    lt: int | None = None
    """The maximum integer."""

    le: int | None = None
    """The maximum integer, inclusive."""

    eq: int | None = None
    """The exact integer."""

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


class StringFilter(GenericFilter[InstrumentedAttribute[str]]):
    """Filter a string column by a range of strings."""

    like: str | None = None
    """The string must match this pattern."""

    ilike: str | None = None
    """The string must match this pattern, case-insensitive."""

    eq: str | None = None
    """The string must be exactly this."""

    def apply(self, query: Select) -> Select:
        if self.like:
            query = query.where(self.value.like(self.like))
        if self.ilike:
            query = query.where(self.value.ilike(self.ilike))
        if self.eq:
            query = query.where(self.value == self.eq)
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
