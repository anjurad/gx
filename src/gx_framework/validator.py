"""Public validation functions for the simplified GX framework."""

from __future__ import annotations

import importlib
import json
import time
from pathlib import Path
from typing import Any

from .config import FrameworkConfig, load_framework_config
from .context_manager import get_gx_context
from .exceptions import SuiteResolutionError, ValidationExecutionError
from .logger import get_logger
from .metrics_store import MetricsPersistenceResult, persist_validation_metrics
from .result_models import build_validation_result_summary
from .sampling import apply_sampling, build_sampling_config
from .suite_resolver import resolve_suite_name
from .utils import build_run_name, infer_dataset_name_from_table


def validate_dataframe(
    df: Any,
    dataset_name: str | None = None,
    suite_name: str | None = None,
    sampling_config: dict[str, Any] | None = None,
    config_path: str | None = None,
    run_name: str | None = None,
    fail_on_error: bool = False,
    save_results: bool = True,
    log_level: str = "INFO",
) -> dict[str, Any]:
    """Validate a Spark DataFrame using a resolved expectation suite.

    Args:
        df: PySpark DataFrame to validate.
        dataset_name: Optional logical dataset name used for suite resolution.
        suite_name: Optional explicit expectation suite name.
        sampling_config: Optional sampling overrides for the validation run.
        config_path: Optional path to validation defaults YAML.
        run_name: Optional explicit run name.
        fail_on_error: Whether to raise when validation fails.
        save_results: Whether to persist a JSON result payload to disk.
        log_level: Logging level override.

    Returns:
        A simplified JSON-serializable validation result dictionary.

    Raises:
        ValidationExecutionError: If fail_on_error is enabled and validation fails.
    """
    config = load_framework_config(config_path=config_path)
    logger = get_logger(config.logs_root, log_level=log_level or config.log_level)
    start_time = time.perf_counter()

    try:
        _validate_dataframe_input(df)
        resolved_suite_name, suite_path = _resolve_suite_with_fallback(
            dataset_name=dataset_name,
            suite_name=suite_name,
            config=config,
        )
        resolved_run_name = run_name or build_run_name(dataset_name, resolved_suite_name)
        original_row_count = _safe_count_rows(df)
        effective_sampling_config = build_sampling_config(config, sampling_config)
        sampled_df, sampling_metadata = apply_sampling(
            df=df,
            sampling_config=effective_sampling_config,
            original_row_count=original_row_count,
        )
        validated_row_count = sampling_metadata.get("validated_row_count", original_row_count)

        logger.info(
            "validation_started",
            extra={
                "dataset_name": dataset_name,
                "suite_name": resolved_suite_name,
                "run_name": resolved_run_name,
                "original_row_count": original_row_count,
                "validated_row_count": validated_row_count,
                "sampling_strategy": sampling_metadata.get("sampling_strategy", "full"),
            },
        )

        context = get_gx_context(config.gx_root)
        validator = _prepare_runtime_batch_request(
            context=context,
            df=sampled_df,
            suite_name=resolved_suite_name,
            run_name=resolved_run_name,
            config=config,
            dataset_name=dataset_name,
        )
        validation_result = _run_validation(
            validator=validator,
            result_format=config.result_format,
        )
        result = _format_result(
            validation_result=validation_result,
            dataset_name=dataset_name,
            suite_name=resolved_suite_name,
            run_name=resolved_run_name,
            original_row_count=original_row_count,
            validated_row_count=validated_row_count,
            sampling_metadata=sampling_metadata,
        )

        metrics_result = _persist_validation_metrics_if_enabled(
            validation_result=validation_result,
            config=config,
            dataset_name=dataset_name,
            suite_name=resolved_suite_name,
            run_name=resolved_run_name,
            row_count=validated_row_count,
            original_row_count=original_row_count,
            sampling_metadata=sampling_metadata,
            validation_time_utc=str(result["validation_time_utc"]),
            logger=logger,
        )

        persisted_result_path = None
        should_save_results = save_results and config.save_validation_results
        if should_save_results:
            persisted_result_path = _save_validation_result(
                logs_root=config.logs_root,
                run_name=resolved_run_name,
                result=result,
            )
            logger.info(
                "validation_result_saved",
                extra={
                    "dataset_name": dataset_name,
                    "suite_name": resolved_suite_name,
                    "run_name": resolved_run_name,
                    "result_path": str(persisted_result_path),
                    "metrics_store_path": (
                        str(metrics_result.metrics_store_path)
                        if metrics_result is not None
                        else None
                    ),
                },
            )

        duration_seconds = round(time.perf_counter() - start_time, 4)
        logger.info(
            "validation_finished",
            extra={
                "dataset_name": dataset_name,
                "suite_name": resolved_suite_name,
                "run_name": resolved_run_name,
                "success": result["success"],
                "failed_expectations": result["failed_expectations"],
                "duration_seconds": duration_seconds,
                "original_row_count": original_row_count,
                "validated_row_count": validated_row_count,
                "sampling_strategy": sampling_metadata.get("sampling_strategy", "full"),
                "suite_path": str(suite_path),
                "result_path": str(persisted_result_path) if persisted_result_path else None,
                "metrics_rows_written": (
                    metrics_result.row_count if metrics_result is not None else 0
                ),
                "metrics_store_path": (
                    str(metrics_result.metrics_store_path)
                    if metrics_result is not None
                    else None
                ),
            },
        )

        if not result["success"] and (fail_on_error or config.fail_on_validation_failure):
            raise ValidationExecutionError(
                f"Validation failed for dataset '{dataset_name or resolved_suite_name}' "
                f"using suite '{resolved_suite_name}'."
            )

        return result
    except Exception as exc:
        logger.exception(
            "validation_exception",
            extra={
                "dataset_name": dataset_name,
                "suite_name": suite_name,
                "run_name": run_name,
            },
        )
        if fail_on_error or isinstance(exc, ValidationExecutionError):
            raise
        return {
            "success": False,
            "dataset_name": dataset_name,
            "suite_name": suite_name,
            "run_name": run_name,
            "total_expectations": 0,
            "successful_expectations": 0,
            "failed_expectations": 0,
            "success_percent": 0.0,
            "validation_time_utc": None,
            "failure_details": [],
            "error_type": type(exc).__name__,
            "error_message": str(exc),
        }


def validate_table(
    table_name: str,
    dataset_name: str | None = None,
    suite_name: str | None = None,
    sampling_config: dict[str, Any] | None = None,
    config_path: str | None = None,
    run_name: str | None = None,
    fail_on_error: bool = False,
    save_results: bool = True,
    log_level: str = "INFO",
) -> dict[str, Any]:
    """Validate a Spark table by reading it from the active Spark session.

    Args:
        table_name: Spark table name to load and validate.
        dataset_name: Optional logical dataset name used for suite resolution.
        suite_name: Optional explicit expectation suite name.
        sampling_config: Optional sampling overrides for the validation run.
        config_path: Optional path to validation defaults YAML.
        run_name: Optional explicit run name.
        fail_on_error: Whether to raise when validation fails.
        save_results: Whether to persist a JSON result payload to disk.
        log_level: Logging level override.

    Returns:
        A simplified JSON-serializable validation result dictionary.
    """
    spark = _get_spark_session()
    resolved_dataset_name = infer_dataset_name_from_table(table_name, dataset_name)
    return validate_dataframe(
        df=spark.table(table_name),
        dataset_name=resolved_dataset_name,
        suite_name=suite_name,
        sampling_config=sampling_config,
        config_path=config_path,
        run_name=run_name,
        fail_on_error=fail_on_error,
        save_results=save_results,
        log_level=log_level,
    )


def _get_spark_session() -> Any:
    """Return the active Spark session or raise a validation error."""
    try:
        spark_session_module = importlib.import_module("pyspark.sql")
        SparkSession = getattr(spark_session_module, "SparkSession")
    except Exception as exc:
        raise ValidationExecutionError(
            "PySpark is not available in this environment."
        ) from exc

    spark = SparkSession.getActiveSession() or SparkSession.getDefaultSession()
    if spark is None:
        raise ValidationExecutionError(
            "No active Spark session is available. Attach or create a Spark session first."
        )
    return spark


def _prepare_runtime_batch_request(
    context: Any,
    df: Any,
    suite_name: str,
    run_name: str,
    config: FrameworkConfig,
    dataset_name: str | None,
) -> Any:
    """Prepare a GX validator for runtime DataFrame validation."""
    try:
        return _get_validator_fluent(
            context=context,
            df=df,
            suite_name=suite_name,
            datasource_name=config.default_datasource_name,
            asset_name=(dataset_name or suite_name) + config.default_data_asset_name_suffix,
        )
    except Exception:
        return _get_validator_legacy(
            context=context,
            df=df,
            suite_name=suite_name,
            datasource_name=config.default_datasource_name,
            run_name=run_name,
        )


def _run_validation(validator: Any, result_format: str) -> dict[str, Any]:
    """Execute a GX validator and normalize its result into a dictionary."""
    try:
        result = validator.validate(result_format=result_format)
    except Exception as exc:
        raise ValidationExecutionError(
            f"Great Expectations validation execution failed: {exc}"
        ) from exc

    if hasattr(result, "to_json_dict"):
        return result.to_json_dict()
    if isinstance(result, dict):
        return result
    raise ValidationExecutionError(
        "Great Expectations returned an unsupported validation result object."
    )


def _format_result(
    validation_result: dict[str, Any],
    dataset_name: str | None,
    suite_name: str,
    run_name: str,
    original_row_count: int | None = None,
    validated_row_count: int | None = None,
    sampling_metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Transform a GX validation result into the public framework contract."""
    result = build_validation_result_summary(
        validation_result=validation_result,
        dataset_name=dataset_name,
        suite_name=suite_name,
        run_name=run_name,
        original_row_count=original_row_count,
        validated_row_count=validated_row_count,
        sampling_metadata=sampling_metadata,
    ).to_dict()
    return result


def _get_validator_fluent(
    context: Any,
    df: Any,
    suite_name: str,
    datasource_name: str,
    asset_name: str,
) -> Any:
    """Create a validator using the modern fluent GX datasource API."""
    data_sources = getattr(context, "data_sources", None)
    if data_sources is None:
        raise ValidationExecutionError("GX fluent data_sources API is unavailable.")

    try:
        spark_datasource = data_sources.get(datasource_name)
    except Exception:
        spark_datasource = data_sources.add_spark(name=datasource_name)

    assets_by_name = {
        getattr(asset, "name", ""): asset
        for asset in (getattr(spark_datasource, "assets", []) or [])
        if getattr(asset, "name", None)
    }
    if asset_name in assets_by_name:
        data_asset = assets_by_name[asset_name]
    else:
        data_asset = spark_datasource.add_dataframe_asset(name=asset_name)

    batch_definitions = {
        getattr(batch_definition, "name", ""): batch_definition
        for batch_definition in (getattr(data_asset, "batch_definitions", []) or [])
        if getattr(batch_definition, "name", None)
    }
    batch_definition_name = "runtime_whole_dataframe"
    if batch_definition_name in batch_definitions:
        batch_definition = batch_definitions[batch_definition_name]
    else:
        batch_definition = data_asset.add_batch_definition_whole_dataframe(
            name=batch_definition_name
        )

    batch_request = batch_definition.build_batch_request(
        batch_parameters={"dataframe": df}
    )
    return context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=suite_name,
    )


def _get_validator_legacy(
    context: Any,
    df: Any,
    suite_name: str,
    datasource_name: str,
    run_name: str,
) -> Any:
    """Create a validator using the legacy RuntimeBatchRequest API."""
    try:
        ge_batch_module = importlib.import_module("great_expectations.core.batch")
        RuntimeBatchRequest = getattr(ge_batch_module, "RuntimeBatchRequest")
    except Exception as exc:
        raise ValidationExecutionError(
            "Great Expectations RuntimeBatchRequest API is unavailable."
        ) from exc

    data_connector_name = "runtime_data_connector"
    try:
        context.get_datasource(datasource_name)
    except Exception:
        context.add_datasource(
            datasource_name,
            class_name="Datasource",
            execution_engine={"class_name": "SparkDFExecutionEngine"},
            data_connectors={
                data_connector_name: {
                    "class_name": "RuntimeDataConnector",
                    "batch_identifiers": ["run_name"],
                }
            },
        )

    batch_request = RuntimeBatchRequest(
        datasource_name=datasource_name,
        data_connector_name=data_connector_name,
        data_asset_name=suite_name,
        runtime_parameters={"batch_data": df},
        batch_identifiers={"run_name": run_name},
    )
    return context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=suite_name,
    )


def _resolve_suite_artifact(gx_root: Path, suite_name: str) -> Path:
    """Resolve the expected suite file path for validation preflight checks."""
    expectations_root = gx_root / "expectations"
    for suffix in (".yml", ".yaml", ".json"):
        candidate = expectations_root / f"{suite_name}{suffix}"
        if candidate.exists():
            return candidate
    raise SuiteResolutionError(
        f"Expectation suite '{suite_name}' was not found under {expectations_root}."
    )


def _resolve_suite_with_fallback(
    dataset_name: str | None,
    suite_name: str | None,
    config: FrameworkConfig,
) -> tuple[str, Path]:
    """Resolve a suite name and verify that an artifact exists for it."""
    primary_suite_name = resolve_suite_name(
        dataset_name=dataset_name,
        suite_name=suite_name,
        datasets_config=config.datasets_config,
        default_suite_suffix=config.default_suite_suffix,
    )

    candidates: list[str] = [primary_suite_name]
    if dataset_name and config.datasets_config and dataset_name in config.datasets_config:
        mapped_suite_name = str(
            config.datasets_config[dataset_name].get("suite_name", "")
        ).strip()
        if mapped_suite_name:
            candidates.append(mapped_suite_name)
        candidates.append(dataset_name)

    seen: set[str] = set()
    for candidate in candidates:
        if not candidate or candidate in seen:
            continue
        seen.add(candidate)
        try:
            return candidate, _resolve_suite_artifact(config.gx_root, candidate)
        except SuiteResolutionError:
            continue

    if suite_name:
        raise SuiteResolutionError(
            f"Explicit expectation suite '{suite_name}' was not found under "
            f"{config.gx_root / 'expectations'}."
        )

    raise SuiteResolutionError(
        f"Could not resolve expectation suite for dataset '{dataset_name}'. Checked "
        "dataset plus suffix, datasets.yml mapping, and plain dataset name."
    )


def _save_validation_result(
    logs_root: Path,
    run_name: str,
    result: dict[str, Any],
) -> Path:
    """Persist the simplified validation result to a JSON file."""
    results_root = logs_root / "validation_results"
    results_root.mkdir(parents=True, exist_ok=True)
    result_path = results_root / f"{run_name}.json"
    result_path.write_text(json.dumps(result, indent=2), encoding="utf-8")
    return result_path


def _persist_validation_metrics_if_enabled(
    validation_result: dict[str, Any],
    config: FrameworkConfig,
    dataset_name: str | None,
    suite_name: str,
    run_name: str,
    row_count: int | None,
    original_row_count: int | None,
    sampling_metadata: dict[str, Any] | None,
    validation_time_utc: str,
    logger: Any,
) -> MetricsPersistenceResult | None:
    """Persist per-expectation metrics when enabled by configuration."""
    if not config.enable_metrics_logging:
        return None

    try:
        metrics_result = persist_validation_metrics(
            validation_result=validation_result,
            config=config,
            dataset_name=dataset_name,
            suite_name=suite_name,
            run_name=run_name,
            row_count=row_count,
            original_row_count=original_row_count,
            sampling_metadata=sampling_metadata,
            validation_time_utc=validation_time_utc,
        )
    except Exception as exc:
        logger.exception(
            "validation_metrics_persistence_failed",
            extra={
                "dataset_name": dataset_name,
                "suite_name": suite_name,
                "run_name": run_name,
                "metrics_store_path": str(config.metrics_store_path),
                "metrics_store_format": config.metrics_store_format,
            },
        )
        if config.metrics_fail_on_persistence_error:
            raise ValidationExecutionError(
                "Validation metrics persistence failed: "
                f"{exc}"
            ) from exc
        return None

    logger.info(
        "validation_metrics_persisted",
        extra={
            "dataset_name": dataset_name,
            "suite_name": suite_name,
            "run_name": run_name,
            "metrics_rows_written": metrics_result.row_count,
            "metrics_store_path": str(metrics_result.metrics_store_path),
            "metrics_store_format": metrics_result.metrics_store_format,
        },
    )
    return metrics_result


def _validate_dataframe_input(df: Any) -> None:
    """Validate that the supplied object looks like a Spark DataFrame."""
    if df is None:
        raise ValidationExecutionError("Input DataFrame cannot be None.")

    spark_session = getattr(df, "sparkSession", None)
    if spark_session is None:
        raise ValidationExecutionError(
            "Input 'df' does not look like a Spark DataFrame because 'sparkSession' is missing."
        )


def _safe_count_rows(df: Any) -> int | None:
    """Best-effort row counting for logging."""
    try:
        return int(df.count())
    except Exception:
        return None