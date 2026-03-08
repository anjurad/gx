"""Normalization helpers for Great Expectations validation outputs."""

from __future__ import annotations

import json
from typing import Any


def normalise_validation_results(
    validation_result: dict[str, Any],
    run_metadata: dict[str, Any],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Flatten GX validation payload into metric rows plus a run summary."""
    results = validation_result.get("results", []) or []
    rows: list[dict[str, Any]] = []

    success_count = 0
    failure_count = 0

    for item in results:
        expectation_config = item.get("expectation_config", {}) or {}
        expectation_type = expectation_config.get("expectation_type")
        result_obj = item.get("result", {}) or {}
        success = bool(item.get("success", False))

        if success:
            success_count += 1
        else:
            failure_count += 1

        row = {
            "run_id": run_metadata["run_id"],
            "run_ts": run_metadata["run_ts"],
            "layer": run_metadata["layer"],
            "schema_name": run_metadata["schema_name"],
            "table_name": run_metadata["table_name"],
            "suite_name": run_metadata["suite_name"],
            "expectation_type": expectation_type,
            "success": success,
            "unexpected_percent": _to_float(result_obj.get("unexpected_percent")),
            "unexpected_count": _to_int(result_obj.get("unexpected_count")),
            "element_count": _to_int(result_obj.get("element_count")),
            "sampling_strategy": run_metadata["sampling_strategy"],
            "original_row_count": _to_int(run_metadata["original_row_count"]),
            "sample_row_count": _to_int(run_metadata["sample_row_count"]),
            "confidence": _to_float(run_metadata["confidence"]),
            "margin_error": _to_float(run_metadata["margin_error"]),
            "stratify_by": run_metadata.get("stratify_by"),
            "details_json": json.dumps(item, default=str),
        }
        rows.append(row)

    summary = {
        "suite_name": run_metadata["suite_name"],
        "run_id": run_metadata["run_id"],
        "success": bool(validation_result.get("success", False)),
        "evaluated_expectations": len(results),
        "successful_expectations": success_count,
        "failed_expectations": failure_count,
        "sampling_strategy": run_metadata["sampling_strategy"],
        "original_row_count": run_metadata["original_row_count"],
        "sample_row_count": run_metadata["sample_row_count"],
        "confidence": run_metadata["confidence"],
        "margin_error": run_metadata["margin_error"],
        "stratify_by": run_metadata.get("stratify_by"),
    }

    return rows, summary


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
