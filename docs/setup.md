# Initial Setup

## Installation

To install Artigraph and all its extra dependencies, run:

```
pip install "artigraph[all]"
```

To install only a select set of dependencies replace `all` with any of:

`aws` `networkx` `numpy` `pandas` `plotly` `polars` `pyarrow` `pydantic`

## Setting up the Database

First, you need to set up an
[async SQLAlchemy engine](https://docs.sqlalchemy.org/en/20/orm/extensions/asyncio.html#sqlalchemy.ext.asyncio.create_async_engine)
and create the Artigraph tables. The quickest way to do this is to use the
[set_engine()][artigraph.set_engine] function, pass it a conntection string or any
engine object, and set `create_tables=True`. You won't need `create_tables=True` if
you're using a database that already has the tables created.

```python
import artigraph as ag

ag.set_engine("sqlite+aiosqlite:///example.db", create_tables=True)
# Do stuff with Artigraph
```

You can also use [current_engine()][artigraph.current_engine] to establish an engine for
use within a particular block of code:

```python
with ag.current_engine("sqlite+aiosqlite:///example.db", create_tables=True):
    # Do stuff with Artigraph
```

!!! note

    You'll need to install `aiosqlite` for the above code to work.
