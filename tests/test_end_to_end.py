from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

import gx_framework.validator as validator_module
from gx_framework.config import FrameworkConfig


class FakeDataFrame:
    def __init__(self) -> None:
        self.sparkSession = object()

    def count(self) -> int:
        return 10


class FakeSparkSession:
    active_session: Any = None

    @classmethod
    def getActiveSession(cls) -> Any:
        return cls.active_session

    @classmethod
    def getDefaultSession(cls) -> Any:
        return cls.active_session


class FakeValidator:
    def validate(self, result_format: str) -> dict[str, Any]:
        assert result_format == "SUMMARY"
        return {
            "success": True,
            "results": [],
            "statistics": {
                "evaluated_expectations": 0,
                "successful_expectations": 0,
                "unsuccessful_expectations": 0,
                "success_percent": 100.0,
            },
        }


def test_validate_table_runs_end_to_end_with_mocked_spark_and_gx(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    gx_root = tmp_path / "gx"
    expectations_root = gx_root / "expectations"
    expectations_root.mkdir(parents=True)
    (gx_root / "great_expectations.yml").write_text("config_version: 4\n", encoding="utf-8")
    (expectations_root / "orders_suite.yml").write_text(
        "expectation_suite_name: orders_suite\n",
        encoding="utf-8",
    )

    fake_dataframe = FakeDataFrame()
    fake_spark = SimpleNamespace(table=lambda table_name: fake_dataframe)
    FakeSparkSession.active_session = fake_spark

    config = FrameworkConfig(
        repo_root=tmp_path,
        gx_root=gx_root,
        logs_root=tmp_path / "logs",
        default_datasource_name="fabric_spark_datasource",
        default_data_asset_name_suffix="_asset",
        default_suite_suffix="_suite",
        default_checkpoint_suffix="_checkpoint",
        save_validation_results=False,
        enable_metrics_logging=False,
        metrics_store_format="delta",
        metrics_store_path=tmp_path / "logs" / "dq_metrics.delta",
        metrics_fail_on_persistence_error=False,
        fail_on_validation_failure=False,
        enable_sampling=False,
        sampling_mode="auto",
        sampling_confidence=0.95,
        sampling_margin_error=0.01,
        sampling_max_rows=100000,
        sampling_stratify_by=None,
        sampling_seed=42,
        log_level="INFO",
        result_format="SUMMARY",
        suite_resolution_order=["explicit_suite_name"],
        datasets_config=None,
    )

    original_import_module = validator_module.importlib.import_module

    def fake_import_module(name: str):
        if name == "pyspark.sql":
            return SimpleNamespace(SparkSession=FakeSparkSession)
        return original_import_module(name)

    monkeypatch.setattr(validator_module, "load_framework_config", lambda config_path=None: config)
    monkeypatch.setattr(validator_module, "get_gx_context", lambda gx_root: object())
    monkeypatch.setattr(
        validator_module,
        "_prepare_runtime_batch_request",
        lambda **kwargs: FakeValidator(),
    )
    monkeypatch.setattr(validator_module.importlib, "import_module", fake_import_module)

    result = validator_module.validate_table(
        table_name="lakehouse.orders",
        dataset_name="orders",
        save_results=False,
    )

    assert result["success"] is True
    assert result["dataset_name"] == "orders"
    assert result["suite_name"] == "orders_suite"