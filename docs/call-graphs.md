# Call Graphs

```python
import asyncio

import pandas as pd
import plotly.express as px
import plotly.graph_objs as go

import artigraph as ag

# we need to register these serializers
import artigraph.extras.pandas
import artigraph.extras.plotly
from artigraph.extras.networkx import create_graph, display_graph

ag.set_engine("sqlite+aiosqlite:///:memory:", create_tables=True)

CSV_URL = "iris.csv"
COL_NAMES = ["sepal_length", "petal_length", "petal_width", "species"]


@ag.trace_function()
def filter_species(df: pd.DataFrame, species: str) -> pd.DataFrame:
    return df[df["species"] == species]


@ag.trace_function()
def filter_petal_length(df: pd.DataFrame, min_length: float) -> pd.DataFrame:
    return df[df["petal_length"] >= min_length]


@ag.trace_function()
def filter_petal_width(df: pd.DataFrame, min_width: float) -> pd.DataFrame:
    return df[df["petal_width"] >= min_width]


@ag.trace_function()
def create_plot(df: pd.DataFrame, x: str, y: str, color: str) -> go.Figure:
    return px.scatter(df, x=x, y=y, color=color)


@ag.trace_function()
def load_filter_and_plot(df: pd.DataFrame) -> go.Figure:
    df = filter_species(df, "Iris-versicolor")
    df = filter_petal_length(df, 4.5)
    df = filter_petal_width(df, 0.5)
    return create_plot(df, "petal_length", "petal_width", "sepal_length")


async def main():
    async with ag.start_trace(ag.Node()) as root:
        df = pd.read_csv(CSV_URL, names=COL_NAMES)
        fig = load_filter_and_plot(df)
        fig.show()
    graph = await create_graph(root)
    display_graph(graph)


if __name__ == "__main__":
    asyncio.run(main())
```
