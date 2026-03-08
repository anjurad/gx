from __future__ import annotations

import pytest

from gx_framework.exceptions import SuiteResolutionError
from gx_framework.suite_resolver import resolve_suite_name


def test_resolve_suite_name_prefers_explicit_name() -> None:
    result = resolve_suite_name(
        dataset_name="customers",
        suite_name="orders_suite",
        datasets_config={"customers": {"suite_name": "customers_suite"}},
        default_suite_suffix="_suite",
    )

    assert result == "orders_suite"


def test_resolve_suite_name_uses_suffix_convention() -> None:
    result = resolve_suite_name(
        dataset_name="customers",
        suite_name=None,
        datasets_config=None,
        default_suite_suffix="_suite",
    )

    assert result == "customers_suite"


def test_resolve_suite_name_uses_dataset_mapping_when_it_differs() -> None:
    result = resolve_suite_name(
        dataset_name="green_tripdata_2017",
        suite_name=None,
        datasets_config={
            "green_tripdata_2017": {"suite_name": "bronze.sales.green_tripdata_2017"}
        },
        default_suite_suffix="_suite",
    )

    assert result == "bronze.sales.green_tripdata_2017"


def test_resolve_suite_name_raises_when_inputs_missing() -> None:
    with pytest.raises(SuiteResolutionError):
        resolve_suite_name(
            dataset_name=None,
            suite_name=None,
            datasets_config=None,
            default_suite_suffix="_suite",
        )