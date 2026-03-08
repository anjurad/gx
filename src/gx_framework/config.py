"""Configuration loading for the simplified GX framework."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from .exceptions import ConfigurationError
from .utils import get_repo_root, normalize_path


DEFAULT_CONFIG: dict[str, Any] = {
    "gx_root": "./gx",
    "logs_root": "./logs",
    "default_datasource_name": "fabric_spark_datasource",
    "default_data_asset_name_suffix": "_asset",
    "default_suite_suffix": "_suite",
    "default_checkpoint_suffix": "_checkpoint",
    "save_validation_results": True,
    "fail_on_validation_failure": False,
    "log_level": "INFO",
    "result_format": "SUMMARY",
    "suite_resolution_order": [
        "explicit_suite_name",
        "dataset_name_plus_suffix",
        "config_mapping",
        "dataset_name",
    ],
}


@dataclass(slots=True)
class FrameworkConfig:
    """Runtime configuration for validation execution.

    Attributes:
        repo_root: Repository root used for relative-path resolution.
        gx_root: Absolute GX project root.
        logs_root: Absolute logs directory.
        default_datasource_name: Default GX datasource name for runtime validation.
        default_data_asset_name_suffix: Suffix applied to runtime DataFrame assets.
        default_suite_suffix: Default suffix used during suite resolution.
        default_checkpoint_suffix: Reserved for compatibility with checkpoint naming.
        save_validation_results: Whether to persist result payloads to disk by default.
        fail_on_validation_failure: Whether failed validations raise by default.
        log_level: Default logging level.
        result_format: GX validation result format.
        suite_resolution_order: Declared suite-resolution order.
        datasets_config: Optional datasets mapping loaded from configuration.
    """

    repo_root: Path
    gx_root: Path
    logs_root: Path
    default_datasource_name: str
    default_data_asset_name_suffix: str
    default_suite_suffix: str
    default_checkpoint_suffix: str
    save_validation_results: bool
    fail_on_validation_failure: bool
    log_level: str
    result_format: str
    suite_resolution_order: list[str]
    datasets_config: dict[str, Any] | None


def load_framework_config(
    config_path: str | Path | None = None,
    repo_root: str | Path | None = None,
) -> FrameworkConfig:
    """Load validation defaults and optional dataset mappings.

    Args:
        config_path: Optional path to the validation defaults YAML file.
        repo_root: Optional repository root for relative-path normalization.

    Returns:
        A normalized framework configuration object.

    Raises:
        ConfigurationError: If the configuration file is explicitly requested but invalid.
    """
    root = normalize_path(repo_root, get_repo_root()) if repo_root else get_repo_root()
    defaults_path = (
        normalize_path(config_path, root)
        if config_path
        else (root / "config" / "validation_defaults.yml")
    )

    config_payload = dict(DEFAULT_CONFIG)
    if defaults_path.exists():
        config_payload.update(_read_yaml_file(defaults_path, required=True))
    elif config_path is not None:
        raise ConfigurationError(
            f"Framework configuration file does not exist: {defaults_path}"
        )

    datasets_path = root / "config" / "datasets.yml"
    datasets_payload = _read_yaml_file(datasets_path, required=False)

    return FrameworkConfig(
        repo_root=root,
        gx_root=normalize_path(config_payload["gx_root"], root),
        logs_root=normalize_path(config_payload["logs_root"], root),
        default_datasource_name=str(config_payload["default_datasource_name"]),
        default_data_asset_name_suffix=str(
            config_payload["default_data_asset_name_suffix"]
        ),
        default_suite_suffix=str(config_payload["default_suite_suffix"]),
        default_checkpoint_suffix=str(config_payload["default_checkpoint_suffix"]),
        save_validation_results=bool(config_payload["save_validation_results"]),
        fail_on_validation_failure=bool(
            config_payload["fail_on_validation_failure"]
        ),
        log_level=str(config_payload["log_level"]).upper(),
        result_format=str(config_payload["result_format"]),
        suite_resolution_order=list(config_payload["suite_resolution_order"]),
        datasets_config=datasets_payload.get("datasets") if datasets_payload else None,
    )


def _read_yaml_file(path: Path, required: bool) -> dict[str, Any]:
    """Read a YAML file into a dictionary.

    Args:
        path: Path to the YAML file.
        required: Whether absence or invalid structure should raise.

    Returns:
        Parsed dictionary or an empty dictionary when optional and absent.

    Raises:
        ConfigurationError: If the file is unreadable or invalid.
    """
    if not path.exists():
        if required:
            raise ConfigurationError(f"Required configuration file is missing: {path}")
        return {}

    try:
        payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except yaml.YAMLError as exc:
        raise ConfigurationError(f"Invalid YAML in configuration file {path}: {exc}") from exc
    except OSError as exc:
        raise ConfigurationError(f"Could not read configuration file {path}: {exc}") from exc

    if not isinstance(payload, dict):
        raise ConfigurationError(
            f"Configuration file {path} must contain a mapping at the top level."
        )
    return payload