from dataclasses import field
from typing import Any, ClassVar, Sequence

from sqlalchemy.orm import Mapped, mapped_column

from artigraph.api.filter import ValueFilter
from artigraph.api.func import Api
from artigraph.api.node import Node
from artigraph.orm.base import OrmBase, make_uuid


def sorted_nodes(nodes: Sequence[Node]) -> Sequence[Node]:
    return sorted(nodes, key=lambda node: node.node_id)


class OrmFake(OrmBase):
    __tablename__ = "fake_table"

    fake_id: Mapped[str] = mapped_column(primary_key=True)
    fake_data: Mapped[str] = mapped_column(nullable=False)


class OrmFakePoly(OrmBase):
    __tablename__ = "fake_poly_table"
    __mapper_args__: ClassVar[dict[str, Any]] = {
        "polymorphic_identity": "poly",
        "polymorphic_on": "fake_poly_id",
    }

    fake_id: Mapped[str] = mapped_column(primary_key=True)
    fake_poly_id: Mapped[str] = mapped_column(nullable=False)


class OrmFakePolyAlpha(OrmFakePoly):
    __mapper_args__: ClassVar[dict[str, Any]] = {"polymorphic_identity": "alpha"}
    fake_alpha: Mapped[str] = mapped_column(nullable=True)


class OrmFakePolyBeta(OrmFakePoly):
    __mapper_args__: ClassVar[dict[str, Any]] = {"polymorphic_identity": "beta"}
    fake_beta: Mapped[str] = mapped_column(nullable=True)


class Fake(Api[OrmFake]):
    orm_type: ClassVar[OrmFake] = OrmFake

    fake_data: str = ""
    fake_id: str = field(default_factory=make_uuid)

    def filters(self) -> dict[type[OrmBase], ValueFilter]:
        return {OrmFake: ValueFilter(eq=self.fake_id).against(OrmFake.fake_id)}

    async def to_orms(self) -> Sequence[OrmBase]:
        return [OrmFake(fake_id=self.fake_id, fake_data=self.fake_data)]

    @classmethod
    async def from_orm(cls, orm: OrmFake) -> "Fake":
        return cls(fake_id=orm.fake_id, fake_data=orm.fake_data, orm=orm)


class FakePoly(Api[OrmFakePoly]):
    orm_type: ClassVar[OrmFakePoly] = OrmFakePoly

    fake_data: str
    fake_id: str = field(default_factory=make_uuid)
    fake_poly_id: str = "poly"

    def filters(self) -> dict[type[OrmBase], ValueFilter]:
        return {OrmFakePoly: ValueFilter(eq=self.fake_id).against(OrmFakePoly.fake_id)}

    async def to_orms(self) -> Sequence[OrmBase]:
        if self.fake_poly_id == "alpha":
            return [
                OrmFakePolyAlpha(
                    fake_id=self.fake_id,
                    fake_alpha=self.fake_data,
                    fake_poly_id=self.fake_poly_id,
                )
            ]
        elif self.fake_poly_id == "beta":
            return [
                OrmFakePolyBeta(
                    fake_id=self.fake_id,
                    fake_beta=self.fake_data,
                    fake_poly_id=self.fake_poly_id,
                )
            ]
        else:
            msg = f"Unknown polymorphic identity {self.fake_poly_id}"
            raise ValueError(msg)

    @classmethod
    async def from_orm(cls, orm: OrmFakePoly) -> "FakePoly":
        if isinstance(orm, OrmFakePolyAlpha):
            return FakePoly(
                fake_id=orm.fake_id,
                fake_data=orm.fake_alpha,
                fake_poly_id=orm.fake_poly_id,
                orm=orm,
            )
        elif isinstance(orm, OrmFakePolyBeta):
            return FakePoly(
                fake_id=orm.fake_id,
                fake_data=orm.fake_beta,
                fake_poly_id=orm.fake_poly_id,
                orm=orm,
            )
        else:
            msg = f"Unknown polymorphic identity {orm.fake_poly_id}"
            raise ValueError(msg)
