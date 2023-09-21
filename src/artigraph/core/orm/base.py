from __future__ import annotations

from dataclasses import KW_ONLY
from datetime import datetime
from typing import Any, ClassVar

from sqlalchemy import func
from sqlalchemy.exc import NoReferencedTableError
from sqlalchemy.inspection import inspect
from sqlalchemy.orm import DeclarativeBase, Mapped, MappedAsDataclass, mapped_column


def get_poly_graph_orm_type(table: str, poly_id: str) -> type[OrmBase]:
    """Get the ORM type for the given table and polymorphic identity."""
    return _ORM_TYPE_BY_TABLE_AND_POLY_ID[(table, poly_id)]


def get_fk_dependency_rank(graph_orm_type: type[OrmBase]) -> int:
    """Get the foreign key dependency rank of the given ORM type.

    This is use to determine the order in which records should be inserted or deleted
    based on their foreign key dependencies.
    """
    return _FK_DEPENDENCY_RANK_BY_TABLE_NAME[graph_orm_type.__tablename__]


class OrmBase(MappedAsDataclass, DeclarativeBase):
    """A base class for all database models."""

    __mapper_args__: ClassVar[dict[str, Any]] = {}

    _: KW_ONLY

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)

        tablename = getattr(cls, "__tablename__", None)
        if not tablename:  # nocov
            return

        if not cls.__mapper_args__.get("polymorphic_abstract"):
            poly_id = cls.__mapper_args__.get("polymorphic_identity")
            if poly_id is not None:
                t_and_p = (cls.__tablename__, poly_id)
                maybe_conflict_cls = _ORM_TYPE_BY_TABLE_AND_POLY_ID.setdefault(t_and_p, cls)
                if cls is not maybe_conflict_cls:  # nocov
                    msg = f"Polymorphic ID {poly_id} exists as {maybe_conflict_cls}"
                    raise ValueError(msg)
        rank = 0
        for c in inspect(cls).columns:
            for fk in c.foreign_keys:
                try:
                    col = fk.column
                except NoReferencedTableError as e:  # nocov
                    msg = "Artigraph does not support deferred foreign keys at this time."
                    raise RuntimeError(msg) from e
                if col.table.name != tablename:
                    other_rank = _FK_DEPENDENCY_RANK_BY_TABLE_NAME[fk.column.table.name] + 1
                    rank = max(rank, other_rank)
        _FK_DEPENDENCY_RANK_BY_TABLE_NAME[tablename] = rank

    created_at: Mapped[datetime] = mapped_column(
        nullable=False,
        default_factory=func.now,
        init=False,
    )
    """The time that this node link was created."""

    updated_at: Mapped[datetime] = mapped_column(
        nullable=False,
        default_factory=func.now,
        onupdate=func.now(),
        init=False,
    )
    """The time that this node link was last updated."""


_ORM_TYPE_BY_TABLE_AND_POLY_ID: dict[tuple[str, str], type[OrmBase]] = {}
"""A mapping from polymorphic identity to ORM type."""

_FK_DEPENDENCY_RANK_BY_TABLE_NAME: dict[str, int] = {}
"""A mapping from table name to FK dependency rank."""
