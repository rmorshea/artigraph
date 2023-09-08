from artigraph.api.filter import ValueFilter
from artigraph.api.funcs import (
    delete_many,
    delete_one,
    exists,
    read,
    read_one,
    write,
    write_one,
)
from tests.common import Fake, FakePoly, OrmFake, OrmFakePoly


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

    await delete_many([fake1, fake2, fake3])
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

    await delete_many([fake1, fake2, fake3])
    assert not await exists(FakePoly, filter_by_fake_ids)
