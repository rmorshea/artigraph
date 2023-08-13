from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable, Protocol, Sequence, TypeVar
from uuid import uuid4

import artigraph as ag

if TYPE_CHECKING:
    from artigraph.model.base import FieldConfig, ModelData

M = TypeVar("M", bound="MarkdownRenderer")

MARKDOWN_RENDERER_BY_TYPE: dict[type[Any], MarkdownRenderer] = {}


class MarkdownRenderer(Protocol):
    """A markdown renderer for a type of value"""

    def __call__(self, key: str, value: str, path: Sequence[str]) -> str:
        ...


def register_renderer(cls: type[Any], *, replace_existing: bool = False) -> Callable[[M], M]:
    """Register a markdown renderer for a type of value"""

    def decorator(renderer: M) -> M:
        if cls in MARKDOWN_RENDERER_BY_TYPE and not replace_existing:
            msg = f"Rrenderer for {cls} already exists, use replace_existing=True to override"
            raise RuntimeError(msg)
        MARKDOWN_RENDERER_BY_TYPE[cls] = renderer
        return renderer

    return decorator


def render(model: ag.BaseModel, *, path: Sequence[str] = ()) -> str:
    """Render a model as markdown"""
    path = path or (uuid4().hex,)
    return _MODEL_TEMPLATE.format(
        ref=path_to_ref(path),
        name=path_to_name(path),
        model_type=model.model_name,
        model_version=model.model_version,
        model_toc=render_model_toc(model, path),
        model_body=render_model_body(model, path),
    )


def render_model_body(model: ag.BaseModel, path: Sequence[str]) -> str:
    """Render the body of a model as markdown"""
    return "\n".join(
        render_model_field(name, value, path) or ""
        for name, (value, _) in sort_fields_first(model.model_data())
    )


def render_model_toc(model: ag.BaseModel, path: Sequence[str]) -> str:
    """Render the table of contents for a model as markdown"""
    toc_rows = [
        render_model_toc_row(name, value, path)
        for name, (value, _) in sort_fields_first(model.model_data())
    ]
    return (
        f"<table><thead><tr><th>Field</th><th>Value</th></tr></thead>"
        f"<tbody>{''.join(toc_rows)}</tbody></table>"
    )


def render_model_toc_row(name: str, value: Any, path: Sequence[str]) -> str:
    return (
        f"<tr><td><a href='#{path_to_ref(path, name)}'>"
        f"<pre>{name}</pre></a></td><td><pre><code>{value}</code></pre></td></tr>"
    )


def render_model_field(name: str, value: Any, path: Sequence[str]) -> str | None:
    """Render a model field as markdown"""
    renderer = MARKDOWN_RENDERER_BY_TYPE.get(type(value))
    if renderer is None:
        return default_render_model_field(name, value, path)
    else:
        return renderer(name, value, path)


def default_render_model_field(name: str, value: Any, path: Sequence[str]) -> str | None:
    """Render a model field as markdown"""
    value = ag.model.base.try_convert_value_to_modeled_type(value)
    if isinstance(value, ag.BaseModel):
        return render(value, path=(*path, name))
    else:
        string = None
        if hasattr(value, "_repr_html_"):
            string = value._repr_html_()
        elif hasattr(value, "_repr_markdown_"):
            string = value._repr_markdown_()
        if string is not None:
            return _MODEL_FIELD_TEMPLATE.format(
                name=path_to_name(path, name),
                ref=path_to_ref(path),
                type=type(value).__name__,
                value=string,
            )


def path_to_ref(path: Sequence[str], *extra: str) -> str:
    return f"{'-'.join((*path, *extra)) or ''}"


def path_to_name(path: Sequence[str], *extra: str) -> str:
    path = [f"[{p}]" if p[:1] in "0123456789" else f".{p}" for p in (*path[1:], *extra) if p]
    return "model" + "".join(path)


def sort_fields_first(model_data: ModelData) -> Sequence[tuple[str, tuple[Any, FieldConfig]]]:
    return sorted(
        model_data.items(), key=lambda i: not isinstance(i[1][0], ag.BaseModel), reverse=True
    )


_MODEL_TEMPLATE = """\
<span id={ref}></span>
<h2><code>{name} : {model_type}-v{model_version}</code></h2>
{model_toc}
<br></br>
{model_body}
"""

_MODEL_FIELD_TEMPLATE = """\
<span id={ref}></span>
<h4><code>{name}: {type}</code></h4>
{value}
<br></br>
"""
