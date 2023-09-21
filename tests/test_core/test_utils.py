import asyncio
from datetime import datetime
from typing import Annotated, Any, Awaitable, cast

import pytest

from artigraph import datetime_serializer, json_serializer
from artigraph.core.api.artifact import SaveSpec
from artigraph.core.utils.anysync import anysync, anysyncmethod
from artigraph.core.utils.misc import UNDEFINED, ExceptionGroup, TaskBatch, slugify
from artigraph.core.utils.type_hints import get_save_specs_from_type_hints


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
        return await cast(Awaitable, some_func())

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


async def test_empty_task_batch():
    assert await TaskBatch().gather() == []


async def test_task_batch_cancel_slow_task_on_error():
    did_cancel = False

    async def slow_task():
        nonlocal did_cancel
        # this will never exit
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            did_cancel = True
            raise

    async def task_with_error():
        raise RuntimeError()

    batch = TaskBatch[None]()
    batch.add(slow_task)
    batch.add(task_with_error)

    with pytest.raises(RuntimeError):
        await batch.gather()

    assert did_cancel


async def test_task_batch_raise_exception_group():
    async def task_with_error():
        raise RuntimeError()

    batch = TaskBatch[int]()
    batch.add(task_with_error)
    batch.add(task_with_error)

    with pytest.raises(ExceptionGroup):
        await batch.gather()


def testget_save_specs_from_type_hints_no_cache():
    def some_func(
        dt: Annotated[datetime | Any, datetime_serializer, json_serializer],  # noqa: ARG001
    ) -> None:
        pass

    assert get_save_specs_from_type_hints(some_func) == {
        "dt": SaveSpec(
            serializers=[datetime_serializer, json_serializer],
            storage=None,
        ),
        "return": SaveSpec(
            serializers=[],
            storage=None,
        ),
    }
