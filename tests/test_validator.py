from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

import gx_framework.validator as validator_module
from gx_framework.config import FrameworkConfig
from gx_framework.exceptions import ValidationExecutionError


class FakeDataFrame:
    def __init__(self, row_count: int = 5) -> None:
        self.sparkSession = object()
        self._row_count = row_count

    def count(self) -> int:
        return self._row_count


class FakeValidator:
    def __init__(self, payload: dict[str, Any]) -> None:
        self.payload = payload

    def validate(self, result_format: str) -> dict[str, Any]:
        assert result_format == "SUMMARY"
        return self.payload


def test_validate_dataframe_returns_required_keys(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _build_config(tmp_path)
    _write_suite_file(config.gx_root, "customers_suite")
    monkeypatch.setattr(validator_module, "load_framework_config", lambda config_path=None: config)
    monkeypatch.setattr(validator_module, "get_gx_context", lambda gx_root: object())
    monkeypatch.setattr(
        validator_module,
        "_prepare_runtime_batch_request",
        lambda **kwargs: FakeValidator(_success_payload()),
    )

    result = validator_module.validate_dataframe(
        df=FakeDataFrame(),
        dataset_name="customers",
    )

    assert set(result) >= {
        "success",
        "dataset_name",
        "suite_name",
        "run_name",
        "total_expectations",
        "successful_expectations",
        "failed_expectations",
        "success_percent",
        "validation_time_utc",
        "failure_details",
    }
    assert result["success"] is True
    assert result["suite_name"] == "customers_suite"
    result_files = list((tmp_path / "logs" / "validation_results").glob("*.json"))
    assert len(result_files) == 1
    assert json.loads(result_files[0].read_text(encoding="utf-8"))["success"] is True


def test_validate_dataframe_includes_failure_details(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _build_config(tmp_path)
    _write_suite_file(config.gx_root, "customers_suite")
    monkeypatch.setattr(validator_module, "load_framework_config", lambda config_path=None: config)
    monkeypatch.setattr(validator_module, "get_gx_context", lambda gx_root: object())
    monkeypatch.setattr(
        validator_module,
        "_prepare_runtime_batch_request",
        lambda **kwargs: FakeValidator(_failure_payload()),
    )

    result = validator_module.validate_dataframe(
        df=FakeDataFrame(),
        dataset_name="customers",
        save_results=False,
    )

    assert result["success"] is False
    assert result["failed_expectations"] == 1
    assert result["failure_details"] == [
        {
            "expectation_type": "expect_column_values_to_not_be_null",
            "column": "customer_id",
            "unexpected_count": 3,
        }
    ]


def test_validate_dataframe_raises_when_fail_on_error_enabled(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _build_config(tmp_path)
    _write_suite_file(config.gx_root, "customers_suite")
    monkeypatch.setattr(validator_module, "load_framework_config", lambda config_path=None: config)
    monkeypatch.setattr(validator_module, "get_gx_context", lambda gx_root: object())
    monkeypatch.setattr(
        validator_module,
        "_prepare_runtime_batch_request",
        lambda **kwargs: FakeValidator(_failure_payload()),
    )

    with pytest.raises(ValidationExecutionError):
        validator_module.validate_dataframe(
            df=FakeDataFrame(),
            dataset_name="customers",
            fail_on_error=True,
            save_results=False,
        )


def _build_config(tmp_path: Path) -> FrameworkConfig:
    gx_root = tmp_path / "gx"
    logs_root = tmp_path / "logs"
    return FrameworkConfig(
        repo_root=tmp_path,
        gx_root=gx_root,
        logs_root=logs_root,
        default_datasource_name="fabric_spark_datasource",
        default_data_asset_name_suffix="_asset",
        default_suite_suffix="_suite",
        default_checkpoint_suffix="_checkpoint",
        save_validation_results=True,
        fail_on_validation_failure=False,
        log_level="INFO",
        result_format="SUMMARY",
        suite_resolution_order=["explicit_suite_name"],
        datasets_config=None,
    )


def _write_suite_file(gx_root: Path, suite_name: str) -> None:
    expectations_root = gx_root / "expectations"
    expectations_root.mkdir(parents=True)
    (gx_root / "great_expectations.yml").write_text("config_version: 4\n", encoding="utf-8")
    (expectations_root / f"{suite_name}.yml").write_text(
        f"expectation_suite_name: {suite_name}\n",
        encoding="utf-8",
    )


def _success_payload() -> dict[str, Any]:
    return {
        "success": True,
        "results": [
            {
                "success": True,
                "expectation_config": {
                    "expectation_type": "expect_table_row_count_to_be_between",
                    "kwargs": {},
                },
                "result": {},
            }
        ],
        "statistics": {
            "evaluated_expectations": 1,
            "successful_expectations": 1,
            "unsuccessful_expectations": 0,
            "success_percent": 100.0,
        },
    }


def _failure_payload() -> dict[str, Any]:
    return {
        "success": False,
        "results": [
            {
                "success": False,
                "expectation_config": {
                    "expectation_type": "expect_column_values_to_not_be_null",
                    "kwargs": {"column": "customer_id"},
                },
                "result": {"unexpected_count": 3},
            }
        ],
        "statistics": {
            "evaluated_expectations": 1,
            "successful_expectations": 0,
            "unsuccessful_expectations": 1,
            "success_percent": 0.0,
        },
    }