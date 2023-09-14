from __future__ import annotations

from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Annotated, Any, TypeVar

from artigraph.core.model.base import GraphModel
from artigraph.core.model.dataclasses import dataclass
from artigraph.core.serializer.datetime import datetime_serializer
from artigraph.core.serializer.json import json_serializer
from artigraph.core.storage.file import FileSystemStorage, temp_file_storage

T = TypeVar("T")
DateTime = Annotated[datetime, datetime_serializer]
Json = Annotated[Any, json_serializer]
TempFileStorage = Annotated[T, temp_file_storage]
tmp_path = TemporaryDirectory()
store1 = FileSystemStorage(Path(tmp_path.name, "store1"))
store2 = FileSystemStorage(Path(tmp_path.name, "store2"))
Store1 = Annotated[T, store1]
Store2 = Annotated[T, store2]


@dataclass
class SimpleDataclassModel(GraphModel, version=1):
    x: int = 1
    y: str = "2"
    z: SimpleDataclassModel | None = None
    dt: DateTime | None = None
    dt_or_json: DateTime | Json | None = None
    dt_with_storage: TempFileStorage[DateTime | None] = None
