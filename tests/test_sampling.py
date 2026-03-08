from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

import gx_framework.sampling as sampling_module
from gx_framework.config import FrameworkConfig


class FakeSampledDataFrame:
    def __init__(self, row_count: int) -> None:
        self.sparkSession = object()
        self._row_count = row_count

    def count(self) -> int:
        return self._row_count


class FakeDataFrame:
    def __init__(self, row_count: int) -> None:
        self.sparkSession = object()
        self._row_count = row_count

    def count(self) -> int:
        return self._row_count

    def orderBy(self, _expr: Any) -> FakeDataFrame:
        return self

    def limit(self, row_count: int) -> FakeSampledDataFrame:
        return FakeSampledDataFrame(row_count)


def test_build_sampling_config_merges_framework_defaults_and_overrides(tmp_path) -> None:
    config = FrameworkConfig(
        repo_root=tmp_path,
        gx_root=tmp_path / "gx",
        logs_root=tmp_path / "logs",
        default_datasource_name="fabric_spark_datasource",
        default_data_asset_name_suffix="_asset",
        default_suite_suffix="_suite",
        default_checkpoint_suffix="_checkpoint",
        save_validation_results=True,
        enable_metrics_logging=False,
        metrics_store_format="delta",
        metrics_store_path=tmp_path / "logs" / "dq_metrics.delta",
        metrics_fail_on_persistence_error=False,
        fail_on_validation_failure=False,
        enable_sampling=True,
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

    merged = sampling_module.build_sampling_config(
        config,
        {"margin_error": 0.02, "max_rows": 50},
    )

    assert merged["enabled"] is True
    assert merged["mode"] == "auto"
    assert merged["margin_error"] == 0.02
    assert merged["max_rows"] == 50


def test_apply_sampling_returns_full_when_disabled() -> None:
    df = FakeDataFrame(row_count=10)

    sampled_df, metadata = sampling_module.apply_sampling(
        df=df,
        sampling_config={"enabled": False},
        original_row_count=10,
    )

    assert sampled_df is df
    assert metadata["sampling_strategy"] == "full"
    assert metadata["original_row_count"] == 10
    assert metadata["validated_row_count"] == 10


def test_apply_sampling_uses_statistical_strategy(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        sampling_module.importlib,
        "import_module",
        lambda name: SimpleNamespace(rand=lambda seed: ("rand", seed)),
    )

    df = FakeDataFrame(row_count=500)
    sampled_df, metadata = sampling_module.apply_sampling(
        df=df,
        sampling_config={
            "enabled": True,
            "mode": "statistical",
            "margin_error": 0.2,
            "max_rows": 25,
            "seed": 7,
        },
        original_row_count=500,
    )

    assert sampled_df.count() == metadata["validated_row_count"]
    assert sampled_df.count() <= 25
    assert sampled_df.count() < 500
    assert metadata["sampling_strategy"] == "statistical"
    assert metadata["original_row_count"] == 500
    assert metadata["validated_row_count"] < 500