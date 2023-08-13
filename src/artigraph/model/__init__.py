# ensure that the builtins are registered
import artigraph.model.builtins  # noqa: F401
from artigraph.model import base

__all__ = ["base"]
