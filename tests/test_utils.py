import pytest

from artigraph.utils import slugify, syncable


def test_syncable():
    @syncable
    async def some_async_func():
        return 42

    assert some_async_func.sync() == 42


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
