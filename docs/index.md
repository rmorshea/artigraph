# Artigraph

Artigraph provides a set of primitives for describing, relating, saving and querying
data artifacts. It is designed to be used in conjunction with existing data pipeline
tools like [Prefect](https://www.prefect.io/) or [Dask](https://dask.org/). Built atop
[SQLAlchemy](https://www.sqlalchemy.org/), it supports most major relational databases
including PostgreSQL, MySQL, and SQLite.

## Installation

```
pip install "artigraph[all]"
```

To install only a select set of dependencies replace `all` with any of:

-   `aws`
-   `pandas`
-   `numpy`
-   `polars`
-   `pyarrow`

## At a Glance

Below is a script that creates an artifact in a local SQLite database and reads it back.

```python
import asyncio

from artigraph import DataModel, ModelGroup, new_node
from artigraph.db import set_engine

# configure what engine artigraph will use
set_engine("sqlite+aiosqlite:///example.db", create_tables=True)


# define a model of your data
class MyDataModel(DataModel, version=1):
    some_value: int
    another_value: dict[str, str]


async def main():
    # create a model group and add a model to it
    async with ModelGroup(new_node()) as group:
        model = MyDataModel(some_value=42, another_value={"foo": "bar"})
        group.add_model("my-data", model)
    # read the model back
    db_model = await group.read_model("my-data", refresh=True)


if __name__ == "__main__":
    asyncio.run(main())
```
