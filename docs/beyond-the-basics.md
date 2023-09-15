# Beyond the Basics

Artigraph focuses on providing a basic [set of primitives](./standard-operations.md) for
working with its core [building blocks](./building-blocks.md). It's up to you to build
higher-level abstractions on top of those primitives. This section provides some ideas
for how you might do that.

!!! note

    The examples below assume you've installed `artigraph[all]` and `aiosqlite`.

    ```bash
    pip install "artigraph[all]" aiosqlite
    ```

## Capturing Intermediate Results

One way that you might use Artigraph is to capture, relate, and then store intermediate
results from functions within a data pipeline. Let's imagine that we have several
functions that are called in sequence to process some data in a
[Pandas DataFrame](https://pandas.pydata.org/). For our purposes, we'll use the
[iris dataset](https://en.wikipedia.org/wiki/Iris_flower_data_set) and perform some
transformations on it. Then we'll create plot of the results with
[Plotly](https://plotly.com/python/).

```python
# main.py
import pandas as pd
import plotly.express as px
import plotly.graph_objs as go

CSV_URL = "https://archive.ics.uci.edu/ml/machine-learning-databases/iris/iris.data"
COL_NAMES = ["sepal_length", "petal_length", "petal_width", "species"]


def filter_species(df: pd.DataFrame, species: str) -> pd.DataFrame:
    return df[df["species"] == species]


def filter_petal_length(df: pd.DataFrame, min_length: float) -> pd.DataFrame:
    return df[df["petal_length"] >= min_length]


def filter_petal_width(df: pd.DataFrame, min_width: float) -> pd.DataFrame:
    return df[df["petal_width"] >= min_width]


def create_plot(df: pd.DataFrame, x: str, y: str, color: str) -> go.Figure:
    return px.scatter(df, x=x, y=y, color=color)


def load_filter_and_plot(df: pd.DataFrame) -> go.Figure:
    df = filter_species(df, "Iris-versicolor")
    df = filter_petal_length(df, 4.5)
    df = filter_petal_width(df, 0.5)
    return create_plot(df, "petal_length", "petal_width", "sepal_length")


if __name__ == "__main__":
    df = pd.read_csv(CSV_URL, names=COL_NAMES)
    fig = load_filter_and_plot(df)
    fig.show()
```

To capture the intermediate results, we'll design a decorator that we can apply to each
function. The purpose of the decortor is to construct a graph of the function calls
along with their inputs and outputs. We'll start by laying out the skeleton of this
decorator:

```python
# capture.py
from functools import wraps
from typing import Any, Callable, TypeVar, cast

import artigraph as ag

F = TypeVar("F", bound=Callable[..., Any])


def capture_graph(func: F) -> F:
    """Capture the inputs and outputs of a function using Artigraph"""

    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        ...

    # Cast here to satisfy your type checker of choice
    return cast(F, wrapper)
```

Next, we'll add to the decorator so it will construct a graph of nodes that mirrors the
calls to functions decorated with `capture_graph`. To do this we'll establish a global
`_CURRENT_NODE` variable that will hold a node representing the currently running
function. Inside the decorator, we'll create a new node and set it as the current node
and link it to the previous current node (if present). Then we'll call the original
function.

```python
# capture.py
from functools import wraps
from typing import Any, Callable, TypeVar, cast

import artigraph as ag

F = TypeVar("F", bound=Callable[..., Any])
_CURRENT_NODE: Optional[ag.Node] = None


def capture_graph(func: F) -> F:
    """Capture the inputs and outputs of a function using Artigraph"""

    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        global _CURRENT_NODE
        last_node = _CURRENT_NODE
        next_node = _CURRENT_NODE = ag.Node()
        try:
            write_before: list[ag.GraphBase] = [next_node]
            if last_node is not None:
                write_before.append(
                    ag.NodeLink(
                        parent_id=last_node.node_id,
                        child_id=next_node.node_id,
                    )
                )
            ag.write_many(write_before)
            return func(*args, **kwargs)
        finally:
            _CURRENT_NODE = last_node

    # Cast here to satisfy your type checker of choice
    return cast(F, wrapper)
```

Finally, we'll add code to capture the inputs and outputs of the function in
[artifacts](./building-blocks.md#artifacts).

```python
# capture.py
from functools import wraps
from typing import Any, Callable, TypeVar, cast

import artigraph as ag

F = TypeVar("F", bound=Callable[..., Any])
_CURRENT_NODE: ag.Node | None = None


def capture_graph(func: F) -> F:
    """Capture the inputs and outputs of a function using Artigraph"""

    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        global _CURRENT_NODE
        last_node = _CURRENT_NODE
        this_node = _CURRENT_NODE = ag.Node()
        try:
            values = {str(i): v for i, v in enumerate(args)} | kwargs
            artifacts_and_links = _create_linked_artifacts(this_node, values)
            write_before: list[ag.GraphBase] = [this_node, *artifacts_and_links]
            if last_node is not None:
                write_before.append(
                    ag.NodeLink(
                        parent_id=last_node.node_id,
                        child_id=this_node.node_id,
                    )
                )
            ag.write_many(write_before)

            result = func(*args, **kwargs)
            write_after = _create_linked_artifacts(this_node, {"return": result})
            ag.write_many(write_after)

            return result
        finally:
            _CURRENT_NODE = last_node

    # Cast here to satisfy your type checker of choice
    return cast(F, wrapper)


def _create_linked_artifacts(parent: ag.Node, values: dict[str, Any]) -> list[ag.GraphBase]:
    """Create a node link for each value in the given dict"""
    records: list[ag.GraphBase] = []
    for k, v in values.items():
        art = ag.Artifact(value=v)
        artlink = ag.NodeLink(
            parent_id=parent.node_id,
            child_id=art.node_id,
            label=k,
        )
        records.extend([art, artlink])
    return records
```

At this point, we can apply the decorator to each function in our pipeline:

```python
# main.py
import pandas as pd
import plotly.express as px
import plotly.graph_objs as go

import artigraph as ag

# we need to register these serializers
import artigraph.extra.serializer.pandas
import artigraph.extra.serializer.plotly

# import our decorator
from capture import capture_graph

ag.set_engine("sqlite+aiosqlite:///example.db", create_tables=True)

CSV_URL = "https://archive.ics.uci.edu/ml/machine-learning-databases/iris/iris.data"
COL_NAMES = ["sepal_length", "petal_length", "petal_width", "species"]


@capture_graph
def filter_species(df: pd.DataFrame, species: str) -> pd.DataFrame:
    return df[df["species"] == species]


@capture_graph
def filter_petal_length(df: pd.DataFrame, min_length: float) -> pd.DataFrame:
    return df[df["petal_length"] >= min_length]


@capture_graph
def filter_petal_width(df: pd.DataFrame, min_width: float) -> pd.DataFrame:
    return df[df["petal_width"] >= min_width]


@capture_graph
def create_plot(df: pd.DataFrame, x: str, y: str, color: str) -> go.Figure:
    return px.scatter(df, x=x, y=y, color=color)


@capture_graph
def load_filter_and_plot(df: pd.DataFrame) -> go.Figure:
    df = filter_species(df, "Iris-versicolor")
    df = filter_petal_length(df, 4.5)
    df = filter_petal_width(df, 0.5)
    return create_plot(df, "petal_length", "petal_width", "sepal_length")


if __name__ == "__main__":
    df = pd.read_csv(CSV_URL, names=COL_NAMES)
    fig = load_filter_and_plot(df)
    fig.show()
```
