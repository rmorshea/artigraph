# Artigraph

[![PyPI - Version](https://img.shields.io/pypi/v/artigraph.svg)](https://pypi.org/project/artigraph)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/artigraph.svg)](https://pypi.org/project/artigraph)

A library for creating interrelated graphs of artifacts and the runs that produce them.

---

**Table of Contents**

- [Installation](#installation)
- [License](#license)

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
does not provide any functionality for running the pipeline itself. Instead, it is meant
to be used in conjunction with a pipeline runner like [Prefect](https://www.prefect.io/).

Artigraph is built atop [SQLAlchemy](https://www.sqlalchemy.org/) using its async
engine. It supports most major databases including PostgreSQL, MySQL, and SQLite.

## Usage

The core concepts in Artigraph are:

- **Artifacts**: The data produced by a pipeline.
- **Artifact Models**: A dataclass that defines the structure of an artifact.
- **Runs**: A collection of artifacts that were produced together.

Under the hood all data is stored in a graph-like representation undef a single `artigraph_node`
table that leverages single table inheritance to store different types of data. This allows
Artigraph to support arbitrary nesting of artifacts and runs without needing to create
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

## Runs

A run allows you to group a collection of artifacts that were produced together:

```python
from artigraph import Run, RunManager

run = Run(node_parent_id=None)
async with RunManager(run) as manager:
    await manager.save_artifact("my-data-model", MyDataModel(...))
```

If you're deep in a call stack and don't want to pass the run manager around, you can
access the currently active manager with the `run_manager()` function:

```python
from artigraph import Run, RunManager


async def my_function():
    await run_manager().save_artifact("my-data-model", MyDataModel(...))


run = Run(node_parent_id=None)
async with RunManager(run) as manager:
    my_function()
```

Runs can be nested and they will automatically inherit the parent run's node ID:

```python
from artigraph import Run, RunManager

run1 = Run(node_parent_id=None)
async with RunManager(run1) as m1:
    run2 = Run(node_parent_id=m1.run.node_id)
    async with RunManager(run2) as m2:
        await m2.save_artifact("my-data-model", MyDataModel(...))
        assert m2.run.node_parent_id == m1.run.node_id
```

Artifacts from nested runs can be loaded:

```python
from artigraph import Run, RunManager

run1 = Run(node_parent_id=None)
async with RunManager(run1) as m1:
    run2 = Run(node_parent_id=m1.run.node_id)
    async with RunManager(run2) as m2:
        await m2.save_artifact("my-data-model", MyDataModel(...))
        assert m2.run.node_parent_id == m1.run.node_id
        ...

run_artifacts = m1.load_descendant_artifacts()
assert run_artifacts == {
    run2.node_id: {"my-data-model": MyDataModel(...)},
    ...
}
```

## Serializers

Artigraph has built-in support for the following data types and serialization formats:

- Pandas DataFrames (`artigraph.serializer.pandas.dataframe_serializer`)
- Numpy Arrays (only 1d and 2d) (`artigraph.serializer.numpy.array_serializer`)
- Polars DataFrames (`artigraph.serializer.polars.dataframe_serializer`)
- PyArrow:
  - Feather (`artigraph.serializer.pyarrow.feather_serializer`)
  - Parque (`artigraph.serializer.pyarrow.parquet_serializer`)

## Storage

Artigraph has built-in support for the following storage backends:

- Local filesystem (`artigraph.storage.file.FileSystem`)
- AWS S3 (`artigraph.storage.aws.S3Storage`)

## License

`artigraph` is distributed under the terms of the [MIT](https://spdx.org/licenses/MIT.html) license.
