# Artigraph

[![PyPI - Version](https://img.shields.io/pypi/v/artigraph.svg)](https://pypi.org/project/artigraph)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/artigraph.svg)](https://pypi.org/project/artigraph)

A library for creating interrelated graphs of artifacts.

---

**Table of Contents**

- [Installation](#installation)
- [About](#about)
- [Usage](#usage)
  - [Artifact Models](#artifact-models)
    - [Nesting Artifact Models](#nesting-artifact-models)
  - [Spans](#spans)
    - [Custom Span Classes](#custom-span-classes)
  - [Serializers](#serializers)
  - [Storage](#storage)

## Installation

```console
pip install "artigraph[all]"
```

To install only a select set of dependencies replace `all` with any of:

- `aws`
- `pandas`
- `numpy`
- `polars`
- `pyarrow`

## About

Artigraph is narrowly focused on managing the artifacts produced by a data pipeline. It
does not provide any functionality for spanning the pipeline itself. Instead, it is meant
to be used in conjunction with a pipeline runner like [Prefect](https://www.prefect.io/)
or [Dask](https://dask.org/).

Artigraph is built atop [SQLAlchemy](https://www.sqlalchemy.org/) using its async
engine. It supports most major databases including PostgreSQL, MySQL, and SQLite.

## Usage

The core concepts in Artigraph are:

- **Artifacts**: The data produced by a pipeline.
- **Artifact Models**: A dataclass that defines the structure of an artifact.
- **Spans**: A collection of artifacts that were produced together.

Under the hood all data is stored in a graph-like representation undef a single `artigraph_node`
table that leverages single table inheritance to store different types of data. This allows
Artigraph to support arbitrary nesting of artifacts and spans without needing to create
additional tables.

## Artifact Models

Define an artifact model like a dataclass:

```python
from dataclasses import dataclass
from artigraph import ArtifactModel


@dataclass
class MyDataModel(ArtifactModel, version=1):
    some_value: int
    another_value: str
```

You can then save to, and load from, the database:

```python
model = MyDataModel(some_value=42, another_value="hello")
artifact_id = await model.save(label="my-data-model")
assert await MyDataModel.load(artifact_id) == model
```

You may specify external storage or custom serializers for model fields. The code below
shows how you might store a large Pandas DataFrame in S3:

```python
import pandas as pd
from dataclasses import dataclass
from artigraph.storage import register_storage
from artigraph.storage.aws import S3Storage
from artigraph.serializer.pandas import pandas_serializer
from artigraph import ArtifactModel, artifact_field

s3_bucket = S3Storage("my-bucket").register()


@dataclass
class MyDataModel(ArtifactModel, version=1):
    some_value: int
    another_value: str
    large_value: pd.DataFrame = artifact_field(storage=s3_bucket, serializer=pandas_serializer)


model = MyDataModel(some_value=42, another_value="hello", large_value=pd.DataFrame({"a": [1, 2, 3]}))
artifact_id = await model.save(label="my-data-model")
```

Default storage and serializers for all fields on a model can be specified with a config:

```python
from dataclasses import dataclass
from artigraph import ArtifactModel, ArtifactModelConfig, artifact_field


@dataclass
class MyDataModel(
    ArtifactModel,
    version=1,
    config=ArtifactModelConfig(
        default_field_storage=s3_bucket,
        default_field_serializer=pandas_serializer,
    )
):
    ...
```

### Nesting Artifact Models

Artifact models can be nested within each other.

```python
from dataclasses import dataclass


@dataclass
class MyDataModel(ArtifactModel, version=1):
    some_value: int
    nested_value: MyDataModel | None = None


model = MyDataModel(some_value=42, nested_value=MyDataModel(some_value=43))
artifact_id = await model.save(label="my-data-model")
assert await MyDataModel.load(artifact_id) == model
```

You can also do this with an `ArtifactMapping` or `ArtifactSequence`:

```python
from dataclasses import dataclass, field


@dataclass
class MyDataModel(ArtifactModel, version=1):
    some_value: int
    nested_map: ArtifactMapping[str, MyDataModel] = field(default_factory=ArtifactMapping)
    nested_seq: ArtifactSequence[MyDataModel] = field(default_factory=ArtifactSequence)


model = MyDataModel(
    some_value=42,
    nested_map=ArtifactMapping(a=MyDataModel(some_value=43)),
    nested_seq=ArtifactSequence([MyDataModel(some_value=44)])
)
artifact_id = await model.save(label="my-data-model")
assert await MyDataModel.load(artifact_id) == model
```

## Spans

A span allows you to group a collection of artifacts that were produced together:

```python
from artigraph import Span, span_context, create_span_artifact

async with span_context():
    await create_span_artifact(span_id="current", label="my-data-model", artifact=MyDataModel(...))
```

You can use `span_id="current"`to automatically detects what the current span is attach an artifact
to it. You can also pass in a span ID manually:

```python
await create_span_artifact(span_id=123, label="my-data-model", artifact=MyDataModel(...))
```

Spans can be nested and they will automatically inherit the parent span's node ID:

```python
from artigraph import Span, span_context

async with span_context():
    async with span_context():
        ...
    async with span_context():
        ...
```

This will construct a graph of spans which looks like:

```
span1
├── span2
└── span3
```

### Custom Span Classes

The `Span` class and associated table is quite barebones and is meant to be subclassed.
Beyond the standard fields defined on the `Node` it only has a `span_opened_at` field
and `span_closed_at` field which are set when a `span_context` begins and ends. You can
extend the `Span` class to add additional fields and relationships using
[single table inheritance](https://docs.sqlalchemy.org/en/20/orm/inheritance.html#single-table-inheritance).
Given that fields on all subclasses of `Node` are stored in the same table, it's
recommended that you prefix your custom fields with the name of your subclass to avoid
collisions.

```python
from sqlalchemy.declarative import Mapped, mapped_column
from artigraph.orm.span import Span


class MySpan(Span):
    __mapper_args__ = {"polymorphic_identity": "my_span"}
    my_span_field: Mapped[str] = mapped_column()
```

You can then use this custom span class with a `span_context`:

```python
from artigraph import Span, SpanManager

my_span = MySpan(node_parent_id=None, my_field="hello")
async with span_context(my_span):
    await create_span_artifact("current", "my-data-model", MyDataModel(...))
```

## Serializers

Artigraph has built-in support for the following data types and serialization formats:

- [Pandas](https://pandas.pydata.org/) DataFrames (`artigraph.serializer.pandas.dataframe_serializer`)
- 1d and 2d [Numpy](https://numpy.org/) Arrays (`artigraph.serializer.numpy.array_serializer`)
- [Polars](https://pola-rs.github.io/) DataFrames (`artigraph.serializer.polars.dataframe_serializer`)
- [PyArrow](https://arrow.apache.org/docs/python/index.html):
  - [Feather](https://arrow.apache.org/docs/format/Columnar.html) (`artigraph.serializer.pyarrow.feather_serializer`)
  - [Parque](https://parquet.apache.org/) (`artigraph.serializer.pyarrow.parquet_serializer`)

## Storage

Artigraph has built-in support for the following storage backends:

- Local filesystem (`artigraph.storage.file.FileSystem`)
- AWS S3 (`artigraph.storage.aws.S3Storage`)

## License

`artigraph` is distributed under the terms of the [MIT](https://spdx.org/licenses/MIT.html) license.
