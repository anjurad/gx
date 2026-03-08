"""Result models for the simplified GX framework."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

from .utils import coerce_float, coerce_int, format_utc_timestamp, utc_now


@dataclass(slots=True)
class FailureDetail:
    """A single expectation failure summary."""

    expectation_type: str
    column: str | None = None
    unexpected_count: int | None = None


@dataclass(slots=True)
class ValidationResultSummary:
    """Serializable validation result returned by the public API."""

    success: bool
    dataset_name: str | None
    suite_name: str
    run_name: str
    total_expectations: int
    successful_expectations: int
    failed_expectations: int
    success_percent: float
    validation_time_utc: str
    original_row_count: int | None = None
    validated_row_count: int | None = None
    sampling_strategy: str = "full"
    sampling_confidence: float | None = None
    sampling_margin_error: float | None = None
    sampling_stratify_by: str | None = None
    failure_details: list[FailureDetail] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Return the result summary as a JSON-serializable dictionary."""
        return asdict(self)


def build_validation_result_summary(
    validation_result: dict[str, Any],
    dataset_name: str | None,
    suite_name: str,
    run_name: str,
    original_row_count: int | None = None,
    validated_row_count: int | None = None,
    sampling_metadata: dict[str, Any] | None = None,
) -> ValidationResultSummary:
    """Build a simplified result summary from a GX validation result.

    Args:
        validation_result: Great Expectations validation result dictionary.
        dataset_name: Dataset name associated with the validation run.
        suite_name: Resolved expectation suite name.
        run_name: Logical run name.

    Returns:
        A normalized validation result summary.
    """
    sampling_metadata = sampling_metadata or {}
    results = validation_result.get("results", []) or []
    successful_expectations = sum(1 for item in results if item.get("success"))
    total_expectations = len(results)
    failed_expectations = total_expectations - successful_expectations
    success_percent = (
        round((successful_expectations / total_expectations) * 100.0, 2)
        if total_expectations
        else 100.0
    )

    failure_details: list[FailureDetail] = []
    for item in results:
        if item.get("success"):
            continue
        expectation_config = item.get("expectation_config", {}) or {}
        kwargs = expectation_config.get("kwargs", {}) or {}
        result_obj = item.get("result", {}) or {}
        expectation_type = expectation_config.get("expectation_type") or expectation_config.get(
            "type"
        )
        failure_details.append(
            FailureDetail(
                expectation_type=str(expectation_type or "unknown_expectation"),
                column=kwargs.get("column"),
                unexpected_count=coerce_int(result_obj.get("unexpected_count")),
            )
        )

    statistics = validation_result.get("statistics", {}) or {}
    total_expectations = coerce_int(statistics.get("evaluated_expectations")) or total_expectations
    successful_expectations = (
        coerce_int(statistics.get("successful_expectations"))
        or successful_expectations
    )
    failed_expectations = (
        coerce_int(statistics.get("unsuccessful_expectations"))
        or failed_expectations
    )
    success_percent = coerce_float(statistics.get("success_percent")) or success_percent

    return ValidationResultSummary(
        success=bool(validation_result.get("success", False)),
        dataset_name=dataset_name,
        suite_name=suite_name,
        run_name=run_name,
        total_expectations=total_expectations,
        successful_expectations=successful_expectations,
        failed_expectations=failed_expectations,
        success_percent=success_percent,
        validation_time_utc=format_utc_timestamp(utc_now()),
        original_row_count=original_row_count,
        validated_row_count=validated_row_count,
        sampling_strategy=str(sampling_metadata.get("sampling_strategy", "full")),
        sampling_confidence=coerce_float(sampling_metadata.get("sampling_confidence")),
        sampling_margin_error=coerce_float(
            sampling_metadata.get("sampling_margin_error")
        ),
        sampling_stratify_by=sampling_metadata.get("sampling_stratify_by"),
        failure_details=failure_details,
    )