from dataclasses import field
from typing import Any, ClassVar, Sequence

from sqlalchemy.orm import Mapped, mapped_column

from artigraph.api.filter import ValueFilter
from artigraph.api.func import Api, delete, delete_one, exists, read, read_one, write, write_one
from artigraph.orm.base import OrmBase, make_uuid


async def test_write_read_delete_one():
    fake = Fake(fake_data="test")
    filter_by_fake_id = ValueFilter(eq=fake.fake_id).against(OrmFake.fake_id)

    await write_one(fake)
    assert await exists(Fake, filter_by_fake_id)

    fake = await read_one(Fake, filter_by_fake_id)
    assert fake.fake_data == "test"

    await delete_one(fake)
    assert not await exists(Fake, filter_by_fake_id)


async def test_write_read_delete_many():
    fake1 = Fake(fake_data="test1")
    fake2 = Fake(fake_data="test2")
    fake3 = Fake(fake_data="test3")

    fake_ids = (fake1.fake_id, fake2.fake_id, fake3.fake_id)
    filter_by_fake_ids = ValueFilter(in_=fake_ids).against(OrmFake.fake_id)

    await write([fake1, fake2, fake3])
    assert await exists(Fake, filter_by_fake_ids)

    fakes = await read(Fake, filter_by_fake_ids)
    assert len(fakes) == 3
    assert {f.fake_data for f in fakes} == {"test1", "test2", "test3"}

    await delete([fake1, fake2, fake3])
    assert not await exists(Fake, filter_by_fake_ids)


async def test_write_read_delete_polymorphic():
    fake1 = FakePoly(fake_data="test1", fake_poly_id="alpha")
    fake2 = FakePoly(fake_data="test2", fake_poly_id="beta")
    fake3 = FakePoly(fake_data="test3", fake_poly_id="alpha")

    fake_ids = (fake1.fake_id, fake2.fake_id, fake3.fake_id)
    filter_by_fake_ids = ValueFilter(in_=fake_ids).against(OrmFakePoly.fake_id)

    await write([fake1, fake2, fake3])
    assert await exists(FakePoly, filter_by_fake_ids)

    fakes = await read(FakePoly, filter_by_fake_ids)
    assert len(fakes) == 3
    assert {f.fake_data for f in fakes} == {"test1", "test2", "test3"}

    await delete([fake1, fake2, fake3])
    assert not await exists(FakePoly, filter_by_fake_ids)


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
