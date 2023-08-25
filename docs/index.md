# Artigraph

Artigraph is a library for creating and working with interrelated graphs of data
artifacts. It's best used in conjunction with existing data pipeline tools like
[Prefect](https://www.prefect.io/) or [Dask](https://dask.org/). Artigraph uses
[SQLAlchemy](https://www.sqlalchemy.org/) and is compatible with most major relational
databases including PostgreSQL, MySQL, and SQLite.

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
-   `plotly`

## At a Glance

Below is a script that creates a data model, saves it in a local SQLite database, and
reads it back.

```python
import asyncio

from artigraph import DataModel, ModelGroup, new_node, set_engine

# configure what engine artigraph will use
set_engine("sqlite+aiosqlite:///example.db", create_tables=True)


# define a model of your data
class MyDataModel(DataModel, version=1):
    some_value: int
    another_value: dict[str, str]


async def main():
    model = MyDataModel(some_value=42, another_value={"foo": "bar"})

    # create a model group and add a model to it
    async with ModelGroup(new_node()) as group:
        group.add_model("my-data", model)

    # read the model back
    db_model = await group.get_model("my-data", fresh=True)


if __name__ == "__main__":
    asyncio.run(main())
```
