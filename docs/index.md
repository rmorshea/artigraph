# Artigraph

A library for describing and relating artifacts.

Artigraph is built atop [SQLAlchemy](https://www.sqlalchemy.org/) and supports most
major databases including PostgreSQL, MySQL, and SQLite. It is designed to be used
in conjunction with existing data pipeline tools like [Prefect](https://www.prefect.io/)
or [Dask](https://dask.org/).

## Installation

```
pip install "artigraph[all]"
```

To install only a select set of dependencies replace `all` with any of:

- `aws`
- `pandas`
- `numpy`
- `polars`
- `pyarrow`

# At a Glance

Below is a script that creates an artifact in a local SQLite database and reads it back.

```python
import asyncio
from dataclasses import dataclass

from artigraph import ArtifactModel
from artigraph.db import set_engine

# configure what engine artigraph will use
set_engine("sqlite+aiosqlite:///example.db", create_tables=True)


# define a model of your data
@dataclass(frozen=True)
class MyData(ArtifactModel, version=1):
    some_value: int
    another_value: dict[str, str]


async def main():
    # construct an artifact
    artifact = MyData(some_value=42, another_value={"foo": "bar"})
    # save it to the database
    artifact_id = await artifact.create(label="my-data")

    # read it back for demonstration purposes
    artifact_from_db = await MyData.read(artifact_id)
    # verify that it's the same as the original
    assert artifact_from_db == artifact


if __name__ == "__main__":
    asyncio.run(main())
```
