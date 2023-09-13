import asyncio

import pytest

from artigraph.core.utils.anysync import (
    anysync,
    anysynccontextmanager,
    anysyncmethod,
)
from artigraph.core.utils.misc import UNDEFINED, TaskBatch, slugify


@pytest.mark.parametrize(
    "raw, slug",
    [
        ("Hello, world!", "hello-world"),
        ("Hello, world!  ", "hello-world"),
        ("Some string with 123 numbers", "some-string-with-123-numbers"),
    ],
)
def test_slugify(raw, slug):
    assert slugify(raw) == slug


def test_undefined_repr():
    assert repr(UNDEFINED) == "UNDEFINED"


async def test_task_batch():
    async def multiply(x, y):
        return x * y

    # test add
    batch = TaskBatch[int]()
    batch.add(multiply, 2, 3)
    batch.add(multiply, 4, 5)
    assert await batch.gather() == [6, 20]

    # test map
    batch = TaskBatch[int]()
    batch.map(multiply, [2, 4], [3, 5])
    assert await batch.gather() == [6, 20]


def test_anysync_functions():
    @anysync
    async def some_func() -> int:
        return 1

    assert some_func.s() == 1
    assert asyncio.run(some_func.a()) == 1
    assert some_func() == 1

    async def async_wrapper():
        return await some_func()

    assert asyncio.run(async_wrapper()) == 1


def test_anysync_functions_with_sync_inner():
    @anysync
    async def some_func() -> int:
        return 1

    async def outer_async():
        return inner_sync()

    def inner_sync():
        return some_func.s()

    assert asyncio.run(outer_async()) == 1


def test_anysyncmethod():
    class SomeClass:
        @anysyncmethod
        async def some_method(self) -> int:
            return 1

    assert SomeClass().some_method.s() == 1
    assert asyncio.run(SomeClass().some_method.a()) == 1
    assert SomeClass().some_method() == 1


async def test_anysynccontextmanager():
    @anysynccontextmanager
    async def some_ctx() -> int:
        yield 1

    async with some_ctx() as x:
        assert x == 1

    with some_ctx() as x:
        assert x == 1
