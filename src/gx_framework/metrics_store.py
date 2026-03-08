"""Persistence helpers for validation metrics."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .config import FrameworkConfig
from .exceptions import ValidationExecutionError


@dataclass(slots=True)
class MetricsPersistenceResult:
    """Outcome of a metrics persistence attempt."""

    row_count: int
    metrics_store_path: Path
    metrics_store_format: str


def persist_validation_metrics(
    validation_result: dict[str, Any],
    config: FrameworkConfig,
    dataset_name: str | None,
    suite_name: str,
    run_name: str,
    row_count: int | None,
    validation_time_utc: str,
    original_row_count: int | None = None,
    sampling_metadata: dict[str, Any] | None = None,
) -> MetricsPersistenceResult:
    """Persist per-expectation validation metrics to the configured store."""
    rows = build_validation_metric_rows(
        validation_result=validation_result,
        dataset_name=dataset_name,
        suite_name=suite_name,
        run_name=run_name,
        row_count=row_count,
        original_row_count=original_row_count,
        sampling_metadata=sampling_metadata,
        validation_time_utc=validation_time_utc,
    )
    if config.metrics_store_format != "delta":
        raise ValidationExecutionError(
            "Unsupported metrics_store_format "
            f"'{config.metrics_store_format}'. Only 'delta' is supported."
        )
    _write_delta_metrics(config.metrics_store_path, rows)
    return MetricsPersistenceResult(
        row_count=len(rows),
        metrics_store_path=config.metrics_store_path,
        metrics_store_format=config.metrics_store_format,
    )


def build_validation_metric_rows(
    validation_result: dict[str, Any],
    dataset_name: str | None,
    suite_name: str,
    run_name: str,
    row_count: int | None,
    validation_time_utc: str,
    original_row_count: int | None = None,
    sampling_metadata: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Flatten a GX validation payload into one metric row per expectation."""
    sampling_metadata = sampling_metadata or {}
    results = validation_result.get("results", []) or []
    statistics = validation_result.get("statistics", {}) or {}
    total_expectations = _to_int(statistics.get("evaluated_expectations")) or len(results)
    successful_expectations = _to_int(statistics.get("successful_expectations"))
    if successful_expectations is None:
        successful_expectations = sum(1 for item in results if item.get("success"))
    failed_expectations = _to_int(statistics.get("unsuccessful_expectations"))
    if failed_expectations is None:
        failed_expectations = total_expectations - successful_expectations

    rows: list[dict[str, Any]] = []
    for item in results:
        expectation_config = item.get("expectation_config", {}) or {}
        kwargs = expectation_config.get("kwargs", {}) or {}
        result_obj = item.get("result", {}) or {}
        expectation_type = expectation_config.get("expectation_type") or expectation_config.get(
            "type"
        )
        rows.append(
            {
                "run_name": run_name,
                "validation_time_utc": validation_time_utc,
                "dataset_name": dataset_name,
                "suite_name": suite_name,
                "run_success": bool(validation_result.get("success", False)),
                "total_expectations": total_expectations,
                "successful_expectations": successful_expectations,
                "failed_expectations": failed_expectations,
                "row_count": row_count,
                "original_row_count": original_row_count,
                "sampling_strategy": sampling_metadata.get("sampling_strategy", "full"),
                "sampling_confidence": _to_float(
                    sampling_metadata.get("sampling_confidence")
                ),
                "sampling_margin_error": _to_float(
                    sampling_metadata.get("sampling_margin_error")
                ),
                "sampling_stratify_by": sampling_metadata.get("sampling_stratify_by"),
                "expectation_type": str(expectation_type or "unknown_expectation"),
                "column": kwargs.get("column"),
                "success": bool(item.get("success", False)),
                "unexpected_percent": _to_float(result_obj.get("unexpected_percent")),
                "unexpected_count": _to_int(result_obj.get("unexpected_count")),
                "element_count": _to_int(result_obj.get("element_count")),
                "details_json": json.dumps(item, default=str),
            }
        )
    return rows


def _write_delta_metrics(
    metrics_store_path: Path,
    metric_rows: list[dict[str, Any]],
) -> None:
    """Append metric rows to a Delta path."""
    metrics_store_path.parent.mkdir(parents=True, exist_ok=True)
    if not metric_rows:
        return

    try:
        import pyarrow as pa
        from deltalake import write_deltalake
    except Exception as exc:
        raise ValidationExecutionError(
            "Delta metrics logging requires optional dependencies 'deltalake' and "
            "'pyarrow'."
        ) from exc

    schema = pa.schema(
        [
            ("run_name", pa.string()),
            ("validation_time_utc", pa.string()),
            ("dataset_name", pa.string()),
            ("suite_name", pa.string()),
            ("run_success", pa.bool_()),
            ("total_expectations", pa.int64()),
            ("successful_expectations", pa.int64()),
            ("failed_expectations", pa.int64()),
            ("row_count", pa.int64()),
            ("original_row_count", pa.int64()),
            ("sampling_strategy", pa.string()),
            ("sampling_confidence", pa.float64()),
            ("sampling_margin_error", pa.float64()),
            ("sampling_stratify_by", pa.string()),
            ("expectation_type", pa.string()),
            ("column", pa.string()),
            ("success", pa.bool_()),
            ("unexpected_percent", pa.float64()),
            ("unexpected_count", pa.int64()),
            ("element_count", pa.int64()),
            ("details_json", pa.string()),
        ]
    )
    columns = {
        field.name: [row.get(field.name) for row in metric_rows]
        for field in schema
    }
    metrics_table = pa.Table.from_pydict(columns, schema=schema)
    write_deltalake(str(metrics_store_path), metrics_table, mode="append")


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None