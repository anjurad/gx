"""Expectation suite name resolution helpers."""

from __future__ import annotations

import logging
from typing import Any

from .exceptions import SuiteResolutionError


LOGGER = logging.getLogger("gx_framework.suite_resolver")


def resolve_suite_name(
    dataset_name: str | None,
    suite_name: str | None,
    datasets_config: dict[str, Any] | None,
    default_suite_suffix: str,
) -> str:
    """Resolve an expectation suite name using the configured precedence.

    Args:
        dataset_name: Optional dataset name supplied by the caller.
        suite_name: Optional explicit suite name supplied by the caller.
        datasets_config: Optional dataset mapping configuration.
        default_suite_suffix: Suffix to append when deriving a suite name.

    Returns:
        A resolved expectation suite name.

    Raises:
        SuiteResolutionError: If no suite name can be resolved.
    """
    if suite_name:
        LOGGER.info(
            "suite_resolution",
            extra={
                "resolution_path": "explicit_suite_name",
                "dataset_name": dataset_name,
                "suite_name": suite_name,
            },
        )
        return suite_name

    mapped_suite_name = ""
    if dataset_name and datasets_config and dataset_name in datasets_config:
        mapped_suite_name = str(datasets_config[dataset_name].get("suite_name", "")).strip()

    if dataset_name:
        resolved_from_suffix = f"{dataset_name}{default_suite_suffix}"
        if not mapped_suite_name or mapped_suite_name == resolved_from_suffix:
            LOGGER.info(
                "suite_resolution",
                extra={
                    "resolution_path": "dataset_name_plus_suffix",
                    "dataset_name": dataset_name,
                    "suite_name": resolved_from_suffix,
                },
            )
            return resolved_from_suffix

    if dataset_name and mapped_suite_name:
        LOGGER.info(
            "suite_resolution",
            extra={
                "resolution_path": "config_mapping",
                "dataset_name": dataset_name,
                "suite_name": mapped_suite_name,
            },
        )
        return mapped_suite_name

    if dataset_name:
        LOGGER.info(
            "suite_resolution",
            extra={
                "resolution_path": "dataset_name",
                "dataset_name": dataset_name,
                "suite_name": dataset_name,
            },
        )
        return dataset_name

    raise SuiteResolutionError(
        "Could not resolve an expectation suite name. Checked explicit suite name, "
        "dataset naming convention, datasets.yml mapping, and plain dataset name."
    )