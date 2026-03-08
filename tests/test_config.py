from __future__ import annotations

from pathlib import Path

import pytest

from gx_framework.config import DEFAULT_CONFIG, load_framework_config
from gx_framework.exceptions import ConfigurationError


def test_load_framework_config_reads_defaults_and_dataset_mapping(tmp_path: Path) -> None:
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    (config_dir / "validation_defaults.yml").write_text(
        "\n".join(
            [
                'gx_root: "./custom_gx"',
                'logs_root: "./custom_logs"',
                'default_datasource_name: "fabric_runtime"',
                'default_data_asset_name_suffix: "_runtime_asset"',
                'default_suite_suffix: "_suite"',
                'default_checkpoint_suffix: "_checkpoint"',
                'save_validation_results: false',
                'enable_metrics_logging: true',
                'metrics_store_format: "delta"',
                'metrics_store_path: "./telemetry/dq_metrics.delta"',
                'metrics_fail_on_persistence_error: true',
                'fail_on_validation_failure: true',
                'enable_sampling: true',
                'sampling_mode: "statistical"',
                'sampling_confidence: 0.9',
                'sampling_margin_error: 0.02',
                'sampling_max_rows: 500',
                'sampling_stratify_by: "customer_segment"',
                'sampling_seed: 99',
                'log_level: "DEBUG"',
                'result_format: "SUMMARY"',
                'suite_resolution_order:',
                '  - explicit_suite_name',
            ]
        ),
        encoding="utf-8",
    )
    (config_dir / "datasets.yml").write_text(
        "datasets:\n  customers:\n    suite_name: customers_suite\n",
        encoding="utf-8",
    )

    config = load_framework_config(repo_root=tmp_path)

    assert config.repo_root == tmp_path.resolve()
    assert config.gx_root == (tmp_path / "custom_gx").resolve()
    assert config.logs_root == (tmp_path / "custom_logs").resolve()
    assert config.default_datasource_name == "fabric_runtime"
    assert config.save_validation_results is False
    assert config.enable_metrics_logging is True
    assert config.metrics_store_format == "delta"
    assert config.metrics_store_path == (tmp_path / "telemetry" / "dq_metrics.delta").resolve()
    assert config.metrics_fail_on_persistence_error is True
    assert config.fail_on_validation_failure is True
    assert config.enable_sampling is True
    assert config.sampling_mode == "statistical"
    assert config.sampling_confidence == 0.9
    assert config.sampling_margin_error == 0.02
    assert config.sampling_max_rows == 500
    assert config.sampling_stratify_by == "customer_segment"
    assert config.sampling_seed == 99
    assert config.log_level == "DEBUG"
    assert config.datasets_config == {"customers": {"suite_name": "customers_suite"}}


def test_load_framework_config_uses_builtin_defaults_when_files_missing(tmp_path: Path) -> None:
    config = load_framework_config(repo_root=tmp_path)

    assert config.gx_root == (tmp_path / "gx").resolve()
    assert config.logs_root == (tmp_path / "logs").resolve()
    assert config.default_suite_suffix == DEFAULT_CONFIG["default_suite_suffix"]
    assert config.enable_metrics_logging is False
    assert config.metrics_store_format == "delta"
    assert config.metrics_store_path == (tmp_path / "logs" / "dq_metrics.delta").resolve()
    assert config.enable_sampling is False
    assert config.sampling_mode == "auto"
    assert config.datasets_config is None


def test_load_framework_config_raises_for_invalid_yaml(tmp_path: Path) -> None:
    config_file = tmp_path / "broken.yml"
    config_file.write_text("gx_root: [", encoding="utf-8")

    with pytest.raises(ConfigurationError):
        load_framework_config(config_path=config_file, repo_root=tmp_path)


def test_metrics_demo_example_config_loads_from_repo() -> None:
    repo_root = Path(__file__).resolve().parents[1]

    config = load_framework_config(
        config_path=repo_root / "config" / "validation_defaults.metrics_demo.yml",
        repo_root=repo_root,
    )

    assert config.enable_metrics_logging is True
    assert config.save_validation_results is False
    assert config.metrics_store_format == "delta"
    assert config.metrics_store_path == (
        repo_root / "logs" / "notebook_demo_dq_metrics.delta"
    ).resolve()
    assert config.metrics_fail_on_persistence_error is True