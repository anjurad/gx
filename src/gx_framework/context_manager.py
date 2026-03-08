"""Great Expectations context loading utilities."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from .exceptions import GXContextError
from .utils import normalize_path


def get_gx_context(gx_root: str | Path) -> Any:
    """Load a Great Expectations context from the configured GX root.

    Args:
        gx_root: GX root directory path.

    Returns:
        A Great Expectations data context instance.

    Raises:
        GXContextError: If the GX root is missing, invalid, or cannot be loaded.
    """
    gx_path = normalize_path(gx_root)
    config_path = gx_path / "great_expectations.yml"

    if not gx_path.exists() or not gx_path.is_dir():
        raise GXContextError(
            f"GX root does not exist or is not a directory: {gx_path}"
        )
    if not config_path.exists():
        raise GXContextError(
            f"Great Expectations configuration file is missing: {config_path}"
        )

    try:
        import great_expectations as gx
    except Exception as exc:
        raise GXContextError(
            "Great Expectations is not available in this environment."
        ) from exc

    get_context = getattr(gx, "get_context", None)
    if get_context is None:
        raise GXContextError("Great Expectations get_context API is unavailable.")

    try:
        return get_context(context_root_dir=str(gx_path))
    except TypeError:
        try:
            return get_context(project_root_dir=str(gx_path))
        except Exception as exc:
            raise GXContextError(
                f"Failed to create Great Expectations context from {gx_path}: {exc}"
            ) from exc
    except Exception as exc:
        raise GXContextError(
            f"Failed to create Great Expectations context from {gx_path}: {exc}"
        ) from exc