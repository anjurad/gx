from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import dq.dq_runner as dq_runner


def test_build_suite_name_convention() -> None:
    assert dq_runner.build_suite_name("bronze", "sales", "customers") == "bronze.sales.customers"


def test_resolve_suite_path_convention(tmp_path: Path) -> None:
    suite = dq_runner.resolve_suite_path(
        "bronze",
        "sales",
        "customers",
        project_root=tmp_path,
    )
    expected = tmp_path / "gx" / "expectations" / "bronze.sales.customers.yml"
    assert suite == expected


def test_run_data_quality_reports_missing_suite(tmp_path: Path) -> None:
    result = dq_runner.run_data_quality(
        df=SimpleNamespace(),
        layer="bronze",
        schema_name="nyc_taxi",
        table_name="green_trips",
        project_root=tmp_path,
    )

    expected_path = tmp_path / "gx" / "expectations" / "bronze.nyc_taxi.green_trips.yml"
    assert result["success"] is False
    assert result["error_type"] == "suite_missing"
    assert "Expectation suite not found:" in result["error_message"]
    assert "bronze.nyc_taxi.green_trips" in result["error_message"]
    assert str(expected_path) in result["error_message"]


def test_run_data_quality_rejects_empty_layer() -> None:
    result = dq_runner.run_data_quality(
        df=SimpleNamespace(),
        layer="",
        schema_name="sales",
        table_name="orders",
    )

    assert result["success"] is False
    assert result["error_type"] == "invalid_input"
    assert "'layer' must be a non-empty string." == result["error_message"]


def test_run_data_quality_reports_environment_error_when_df_not_spark(
    tmp_path: Path,
    monkeypatch,
) -> None:
    expectations_dir = tmp_path / "gx" / "expectations"
    expectations_dir.mkdir(parents=True)
    suite_file = expectations_dir / "bronze.sales.orders.yml"
    suite_file.write_text("expectation_suite_name: bronze.sales.orders\n", encoding="utf-8")

    original_import_module = dq_runner.importlib.import_module

    def _mock_import_module(name: str):
        if name in {"great_expectations", "pyspark"}:
            return object()
        return original_import_module(name)

    monkeypatch.setattr(dq_runner.importlib, "import_module", _mock_import_module)

    result = dq_runner.run_data_quality(
        df=SimpleNamespace(),
        layer="bronze",
        schema_name="sales",
        table_name="orders",
        project_root=tmp_path,
    )

    assert result["success"] is False
    assert result["error_type"] == "environment_error"
    assert "sparkSession" in result["error_message"]


def test_run_data_quality_rejects_unknown_storage_profile() -> None:
    result = dq_runner.run_data_quality(
        df=SimpleNamespace(),
        layer="bronze",
        schema_name="sales",
        table_name="orders",
        storage_profile="adls_gen2",
    )

    assert result["success"] is False
    assert result["error_type"] == "invalid_input"
    assert "storage_profile" in result["error_message"]


def test_run_data_quality_rejects_unknown_source_format() -> None:
    result = dq_runner.run_data_quality(
        df=SimpleNamespace(),
        layer="bronze",
        schema_name="sales",
        table_name="orders",
        source_format="orc",
    )

    assert result["success"] is False
    assert result["error_type"] == "invalid_input"
    assert "source_format" in result["error_message"]


def test_run_data_quality_rejects_unknown_metrics_sink() -> None:
    result = dq_runner.run_data_quality(
        df=SimpleNamespace(),
        layer="bronze",
        schema_name="sales",
        table_name="orders",
        metrics_sink="filesystem",
    )

    assert result["success"] is False
    assert result["error_type"] == "invalid_input"
    assert "metrics_sink" in result["error_message"]


def test_run_data_quality_rejects_unknown_metrics_format() -> None:
    result = dq_runner.run_data_quality(
        df=SimpleNamespace(),
        layer="bronze",
        schema_name="sales",
        table_name="orders",
        metrics_format="json",
    )

    assert result["success"] is False
    assert result["error_type"] == "invalid_input"
    assert "metrics_format" in result["error_message"]


def test_run_data_quality_requires_metrics_path_when_sink_is_path() -> None:
    result = dq_runner.run_data_quality(
        df=SimpleNamespace(),
        layer="bronze",
        schema_name="sales",
        table_name="orders",
        metrics_sink="path",
        dq_metrics_path=None,
    )

    assert result["success"] is False
    assert result["error_type"] == "invalid_input"
    assert "dq_metrics_path" in result["error_message"]
