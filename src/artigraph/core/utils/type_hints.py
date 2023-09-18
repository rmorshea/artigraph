from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from typing import (
    Annotated,
    Any,
    Sequence,
    get_args,
    get_origin,
    get_type_hints,
)

from artigraph.core.serializer.base import Serializer
from artigraph.core.storage.base import Storage


def get_artigraph_type_hint_metadata(
    obj: Any, *, use_cache: bool = False
) -> dict[str, TypeHintMetadata]:
    """Get the model data for a dataclass-like instance."""
    hints = (_cached_get_type_hints if use_cache else get_type_hints)(obj)
    info: dict[str, TypeHintMetadata] = {}
    for name, anno in hints.items():
        serializers: list[Serializer] = []
        storage: Storage | None = None
        for anno_meta in _find_all_annotated_metadata(anno):
            for arg in get_args(anno_meta):
                if isinstance(arg, Serializer):
                    serializers.append(arg)
                elif isinstance(arg, Storage):
                    if storage is not None:
                        msg = f"Multiple storage types specified for {name!r} - {arg} and {storage}"
                        raise ValueError(msg)
                    storage = arg
        info[name] = TypeHintMetadata(serializers, storage)
    return info


@dataclass(frozen=True)
class TypeHintMetadata:
    serializers: Sequence[Serializer] = ()
    storage: Storage | None = None


def _find_all_annotated_metadata(hint: Any) -> Sequence[Annotated]:
    """Find all Annotated metadata in a type hint."""
    if get_origin(hint) is Annotated:
        return [hint, *[a for h in get_args(hint) for a in _find_all_annotated_metadata(h)]]
    return [a for h in get_args(hint) for a in _find_all_annotated_metadata(h)]


@lru_cache(maxsize=None)
def _cached_get_type_hints(cls: type[Any]) -> dict[str, Any]:
    # This can be pretty slow and there should be a finite number of classes so we cache it.
    # We need to be careful about this though because we don't have a max cache size.
    return get_type_hints(cls, include_extras=True)
