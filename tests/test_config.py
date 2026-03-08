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
                'fail_on_validation_failure: true',
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
    assert config.fail_on_validation_failure is True
    assert config.log_level == "DEBUG"
    assert config.datasets_config == {"customers": {"suite_name": "customers_suite"}}


def test_load_framework_config_uses_builtin_defaults_when_files_missing(tmp_path: Path) -> None:
    config = load_framework_config(repo_root=tmp_path)

    assert config.gx_root == (tmp_path / "gx").resolve()
    assert config.logs_root == (tmp_path / "logs").resolve()
    assert config.default_suite_suffix == DEFAULT_CONFIG["default_suite_suffix"]
    assert config.datasets_config is None


def test_load_framework_config_raises_for_invalid_yaml(tmp_path: Path) -> None:
    config_file = tmp_path / "broken.yml"
    config_file.write_text("gx_root: [", encoding="utf-8")

    with pytest.raises(ConfigurationError):
        load_framework_config(config_path=config_file, repo_root=tmp_path)