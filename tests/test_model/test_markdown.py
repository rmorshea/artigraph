from dataclasses import field

import pandas as pd

from artigraph import DataModel
from artigraph.model.markdown import render


class ModelForMarkdownRender(DataModel, version=1):
    x: int
    y: int
    inner: "ModelForMarkdownRender | None" = None
    df: pd.DataFrame = field(default_factory=pd.DataFrame, repr=False)


def test_render_without_error():
    model = ModelForMarkdownRender(
        x=1,
        y=2,
        inner=ModelForMarkdownRender(x=3, y=4),
        df=pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]}),
    )
    render(model)
