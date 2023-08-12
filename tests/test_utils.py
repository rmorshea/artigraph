import pytest

from artigraph.utils import UNDEFINED, TaskBatch, slugify


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
