from importlib import import_module
from logging import getLogger
from pathlib import Path

_logger = getLogger(__name__)
_MODULE_NAMES: set[str] = {
    path.stem for path in Path(__file__).parent.glob("*.py") if path.stem != "__init__"
}


def load_extras(*names: str) -> None:  # nocov
    """Load extra modules.

    This is useful for registering serializers from 3rd party libraries.
    """
    invalid = set(names).difference(_MODULE_NAMES)
    if invalid:
        msg = f"Invalid module names: {invalid}"
        raise ValueError(msg)

    for n in names or _MODULE_NAMES:
        try:
            import_module(f"artigraph.extras.{n}")
        except ImportError:
            if names:
                raise
            _logger.debug(f"Failed to load artigraph.extras.{n}", exc_info=True)
