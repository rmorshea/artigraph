from dataclasses import field
from typing import Any, ClassVar, Sequence
from uuid import UUID, uuid1

from sqlalchemy.orm import Mapped, mapped_column
from typing_extensions import Self

from artigraph.core.api.base import GraphObject
from artigraph.core.api.filter import Filter, ValueFilter
from artigraph.core.api.node import Node
from artigraph.core.orm.base import OrmBase
from artigraph.core.utils.misc import FrozenDataclass


def sorted_nodes(nodes: Sequence[Node]) -> Sequence[Node]:
    return sorted(nodes, key=lambda node: node.graph_id)


class OrmFake(OrmBase):
    __tablename__ = "fake_table"

    fake_id: Mapped[UUID] = mapped_column(primary_key=True)
    fake_data: Mapped[str] = mapped_column(nullable=False)


class OrmFakePoly(OrmBase):
    __tablename__ = "fake_poly_table"
    __mapper_args__: ClassVar[dict[str, Any]] = {
        "polymorphic_identity": "poly",
        "polymorphic_on": "fake_poly_id",
    }

    fake_id: Mapped[UUID] = mapped_column(primary_key=True)
    fake_poly_id: Mapped[str] = mapped_column(nullable=False)


class OrmFakePolyAlpha(OrmFakePoly):
    __mapper_args__: ClassVar[dict[str, Any]] = {"polymorphic_identity": "alpha"}
    fake_alpha: Mapped[str] = mapped_column(nullable=True)


class OrmFakePolyBeta(OrmFakePoly):
    __mapper_args__: ClassVar[dict[str, Any]] = {"polymorphic_identity": "beta"}
    fake_beta: Mapped[str] = mapped_column(nullable=True)


class Fake(FrozenDataclass, GraphObject[OrmFake, Any, Filter]):
    graph_orm_type: ClassVar[type[OrmFake]] = OrmFake

    fake_data: str = ""
    fake_id: UUID = field(default_factory=uuid1)

    def graph_filter_self(self) -> ValueFilter:
        return ValueFilter(eq=self.fake_id).against(OrmFake.fake_id)

    @classmethod
    def graph_filter_related(cls, _: ValueFilter) -> dict:
        return {}

    async def graph_dump_self(self) -> OrmFake:
        return OrmFake(fake_id=self.fake_id, fake_data=self.fake_data)

    async def graph_dump_related(self) -> Sequence[Any]:
        return []

    @classmethod
    async def graph_load(cls, records: Sequence[OrmFake], _: dict) -> Sequence[Self]:
        return [cls(fake_id=r.fake_id, fake_data=r.fake_data) for r in records]


class FakePoly(FrozenDataclass, GraphObject[OrmFakePoly, Any, Filter]):
    graph_orm_type: ClassVar[type[OrmFakePoly]] = OrmFakePoly

    fake_data: str
    fake_id: UUID = field(default_factory=uuid1)
    fake_poly_id: str = "poly"

    def graph_filter_self(self) -> ValueFilter:
        return ValueFilter(eq=self.fake_id).against(OrmFakePoly.fake_id)

    @classmethod
    def graph_filter_related(cls, _: ValueFilter) -> dict:
        return {}

    async def graph_dump_self(self) -> OrmFakePoly:
        if self.fake_poly_id == "alpha":
            return OrmFakePolyAlpha(
                fake_id=self.fake_id,
                fake_alpha=self.fake_data,
                fake_poly_id=self.fake_poly_id,
            )

        elif self.fake_poly_id == "beta":
            return OrmFakePolyBeta(
                fake_id=self.fake_id,
                fake_beta=self.fake_data,
                fake_poly_id=self.fake_poly_id,
            )

        else:
            msg = f"Unknown polymorphic identity {self.fake_poly_id}"
            raise ValueError(msg)

    async def graph_dump_related(self) -> Sequence[Any]:
        return []

    @classmethod
    async def graph_load(cls, records: Sequence[OrmFakePoly], _: dict) -> Sequence[Self]:
        objs: list[Self] = []
        for record in records:
            if isinstance(record, OrmFakePolyAlpha):
                objs.append(
                    cls(
                        fake_id=record.fake_id,
                        fake_data=record.fake_alpha,
                        fake_poly_id=record.fake_poly_id,
                    )
                )
            elif isinstance(record, OrmFakePolyBeta):
                objs.append(
                    cls(
                        fake_id=record.fake_id,
                        fake_data=record.fake_beta,
                        fake_poly_id=record.fake_poly_id,
                    )
                )
            else:
                msg = f"Unknown polymorphic identity {record.fake_poly_id}"
                raise ValueError(msg)
        return objs
