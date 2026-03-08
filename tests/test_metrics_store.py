from __future__ import annotations

from pathlib import Path

import pytest

from gx_framework.config import FrameworkConfig
from gx_framework.metrics_store import persist_validation_metrics


def test_persist_validation_metrics_writes_delta_table(tmp_path: Path) -> None:
    pytest.importorskip("pyarrow")
    deltalake = pytest.importorskip("deltalake")

    metrics_path = tmp_path / "logs" / "dq_metrics.delta"
    config = FrameworkConfig(
        repo_root=tmp_path,
        gx_root=tmp_path / "gx",
        logs_root=tmp_path / "logs",
        default_datasource_name="fabric_spark_datasource",
        default_data_asset_name_suffix="_asset",
        default_suite_suffix="_suite",
        default_checkpoint_suffix="_checkpoint",
        save_validation_results=True,
        enable_metrics_logging=True,
        metrics_store_format="delta",
        metrics_store_path=metrics_path,
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

    validation_result = {
        "success": False,
        "results": [
            {
                "success": False,
                "expectation_config": {
                    "expectation_type": "expect_column_values_to_not_be_null",
                    "kwargs": {"column": "customer_id"},
                },
                "result": {
                    "unexpected_count": 2,
                    "unexpected_percent": 25.0,
                    "element_count": 8,
                },
            }
        ],
        "statistics": {
            "evaluated_expectations": 1,
            "successful_expectations": 0,
            "unsuccessful_expectations": 1,
            "success_percent": 0.0,
        },
    }

    persistence_result = persist_validation_metrics(
        validation_result=validation_result,
        config=config,
        dataset_name="customers",
        suite_name="customers_suite",
        run_name="customers_20260308_120000",
        row_count=8,
        original_row_count=12,
        sampling_metadata={
            "sampling_strategy": "statistical",
            "sampling_confidence": 0.95,
            "sampling_margin_error": 0.01,
            "sampling_stratify_by": None,
        },
        validation_time_utc="2026-03-08T12:00:00Z",
    )

    delta_table = deltalake.DeltaTable(str(metrics_path))
    records = delta_table.to_pyarrow_table().to_pylist()

    assert persistence_result.row_count == 1
    assert persistence_result.metrics_store_path == metrics_path
    assert records[0]["dataset_name"] == "customers"
    assert records[0]["original_row_count"] == 12
    assert records[0]["sampling_strategy"] == "statistical"
    assert records[0]["expectation_type"] == "expect_column_values_to_not_be_null"
    assert records[0]["unexpected_count"] == 2