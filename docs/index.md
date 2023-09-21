# Artigraph

Artigraph is a library for creating and working with interrelated graphs of data
artifacts. It's best used in conjunction with data pipeline tools like
[Prefect](https://www.prefect.io/) or [Dask](https://dask.org/). Under the hood,
Artigraph uses [SQLAlchemy](https://www.sqlalchemy.org/) and is compatible with most
major relational databases including PostgreSQL, MySQL, and SQLite. It also supports
popular tools like [Pydantic](https://docs.pydantic.dev/), [Numpy](https://numpy.org/),
and [Pandas](https://pandas.pydata.org/).

## At a Glance

The script below creates a graph of data artifacts and displays it using
[NetworkX](https://networkx.org/) and [Plotly](https://plotly.com/).

!!! note

    Running this script will require you to `pip install "artigraph[networkx,plotly]" aiosqlite`

```python
import artigraph as ag
from artigraph.extras.networkx import create_graph
from artigraph.extras.plotly import figure_from_networkx

# configure what engine artigraph will use
ag.set_engine("sqlite+aiosqlite:///example.db", create_tables=True)


# define a model of your data
@ag.dataclass
class MyModel(ag.GraphModel, version=1):
    some_value: int
    another_value: dict[str, str]


# create a node, a model, and a link between them
node = ag.Node()
model = MyModel(some_value=42, another_value={"foo": "bar"})
link = ag.Link(source_id=node.graph_id, target_id=model.graph_id, label="my_model")

# write them to the database
ag.write_many([node, model, link])

# create and display the resulting graph
graph = create_graph(node)
fig = figure_from_networkx(graph)
fig.show()
```
