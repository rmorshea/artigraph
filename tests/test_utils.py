import pytest

from artigraph.utils import UNDEFINED, slugify


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
