"""Shared utility helpers for the simplified GX framework."""

from __future__ import annotations

import datetime as dt
from pathlib import Path
from typing import Any


def get_repo_root() -> Path:
    """Resolve the repository root from the installed package location.

    Returns:
        The repository root path.
    """
    return Path(__file__).resolve().parents[2]


def normalize_path(path_value: str | Path, base_path: str | Path | None = None) -> Path:
    """Return an absolute path, resolving relative values against a base path.

    Args:
        path_value: Path value to normalize.
        base_path: Optional base path for relative values.

    Returns:
        A normalized absolute path.
    """
    path = Path(path_value).expanduser()
    if path.is_absolute():
        return path

    base = Path(base_path) if base_path else get_repo_root()
    return (base / path).resolve()


def utc_now() -> dt.datetime:
    """Return the current timezone-aware UTC timestamp."""
    return dt.datetime.now(dt.UTC)


def format_utc_timestamp(value: dt.datetime) -> str:
    """Format a timezone-aware timestamp as an ISO 8601 UTC string."""
    normalized = value.astimezone(dt.UTC).replace(microsecond=0)
    return normalized.isoformat().replace("+00:00", "Z")


def build_run_name(dataset_name: str | None, suite_name: str) -> str:
    """Build a deterministic run name from the dataset or suite name.

    Args:
        dataset_name: Optional dataset name supplied by the caller.
        suite_name: Resolved expectation suite name.

    Returns:
        A run name suitable for logs and result persistence.
    """
    base_name = dataset_name or suite_name
    timestamp = utc_now().strftime("%Y%m%d_%H%M%S")
    return f"{base_name}_{timestamp}"


def coerce_int(value: Any) -> int | None:
    """Convert a value to int when possible."""
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def coerce_float(value: Any) -> float | None:
    """Convert a value to float when possible."""
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def infer_dataset_name_from_table(table_name: str, dataset_name: str | None) -> str:
    """Infer a dataset name when only a Spark table name is provided.

    Args:
        table_name: Spark table name used for validation.
        dataset_name: Optional explicit dataset name.

    Returns:
        The explicit dataset name or the table name suffix.
    """
    if dataset_name:
        return dataset_name
    return table_name.rsplit(".", maxsplit=1)[-1]