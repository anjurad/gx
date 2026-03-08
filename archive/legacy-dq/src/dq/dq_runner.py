"""Core DQ orchestration for Great Expectations + PySpark."""

from __future__ import annotations

import datetime as dt
import importlib
import uuid
from pathlib import Path
from typing import Any

from dq.normalise import normalise_validation_results
from dq.sampling import apply_sampling


SUPPORTED_STORAGE_PROFILES = {"local_fs", "azurite_blob"}
SUPPORTED_SOURCE_FORMATS = {"csv", "parquet", "delta"}
SUPPORTED_METRICS_FORMATS = {"delta", "parquet"}
SUPPORTED_METRICS_SINKS = {"table", "path"}


def build_suite_name(layer: str, schema_name: str, table_name: str) -> str:
    """Build deterministic suite name from lakehouse coordinates."""
    return f"{layer}.{schema_name}.{table_name}"


def resolve_suite_path(
    layer: str,
    schema_name: str,
    table_name: str,
    project_root: str | Path | None = None,
) -> Path:
    """Resolve expectation suite path using deterministic naming only."""
    root = Path(project_root) if project_root else _default_project_root()
    suite_name = build_suite_name(layer, schema_name, table_name)
    return root / "gx" / "expectations" / f"{suite_name}.yml"


def run_data_quality(
    df: Any,
    layer: str,
    schema_name: str,
    table_name: str,
    sampling_config: dict[str, Any] | None = None,
    log_metrics: bool = True,
    dq_metrics_table: str = "analytics.dq_metrics",
    dq_metrics_path: str | None = None,
    storage_profile: str = "local_fs",
    source_format: str = "csv",
    metrics_sink: str = "table",
    metrics_format: str = "delta",
    project_root: str | Path | None = None,
) -> dict[str, Any]:
    """Run DQ lifecycle: discovery, sampling, GX validation, normalization, logging."""
    validation_error = _validate_inputs(
        df,
        layer,
        schema_name,
        table_name,
        storage_profile,
        source_format,
        metrics_sink,
        metrics_format,
        dq_metrics_path,
    )
    if validation_error:
        return validation_error

    suite_name = build_suite_name(layer, schema_name, table_name)
    suite_path = resolve_suite_path(layer, schema_name, table_name, project_root=project_root)

    if not suite_path.exists():
        return _suite_missing_error(layer, schema_name, table_name, suite_name, suite_path)

    environment_error = _preflight_environment(df, suite_path)
    if environment_error:
        return {
            "success": False,
            "error_type": "environment_error",
            "error_message": environment_error,
            "suite_name": suite_name,
            "layer": layer,
            "schema_name": schema_name,
            "table_name": table_name,
            "storage_profile": storage_profile,
            "source_format": source_format,
            "metrics_sink": metrics_sink,
            "metrics_format": metrics_format,
            "dq_metrics_path": dq_metrics_path,
        }

    yaml_error = _validate_suite_yaml(suite_path, suite_name)
    if yaml_error:
        return {
            "success": False,
            "error_type": "suite_invalid",
            "error_message": yaml_error,
            "suite_name": suite_name,
            "layer": layer,
            "schema_name": schema_name,
            "table_name": table_name,
        }

    sampled_df, sample_meta = apply_sampling(df, sampling_config)

    run_metadata = {
        "run_id": str(uuid.uuid4()),
        "run_ts": dt.datetime.now(dt.UTC),
        "layer": layer,
        "schema_name": schema_name,
        "table_name": table_name,
        "suite_name": suite_name,
        "sampling_strategy": sample_meta["sampling_strategy"],
        "original_row_count": sample_meta["original_row_count"],
        "sample_row_count": sample_meta["sample_row_count"],
        "confidence": sample_meta["confidence"],
        "margin_error": sample_meta["margin_error"],
        "stratify_by": sample_meta["stratify_by"],
    }

    try:
        validation_result = _run_gx_validation(sampled_df, suite_name, run_metadata["run_id"], suite_path)
    except Exception as exc:
        return {
            "success": False,
            "error_type": "validation_error",
            "error_message": str(exc),
            **run_metadata,
        }

    metric_rows, summary = normalise_validation_results(validation_result, run_metadata)

    if log_metrics:
        try:
            written_rows = _write_metrics(
                sampled_df.sparkSession,
                metric_rows,
                dq_metrics_table,
                metrics_sink=metrics_sink,
                metrics_format=metrics_format,
                dq_metrics_path=dq_metrics_path,
            )
            summary["metrics_logged"] = True
            summary["metrics_rows_written"] = written_rows
            summary["dq_metrics_table"] = dq_metrics_table
            summary["dq_metrics_path"] = dq_metrics_path
            summary["metrics_sink"] = metrics_sink
            summary["metrics_format"] = metrics_format
        except Exception as exc:
            summary["metrics_logged"] = False
            summary["metrics_rows_written"] = 0
            summary["dq_metrics_table"] = dq_metrics_table
            summary["dq_metrics_path"] = dq_metrics_path
            summary["metrics_sink"] = metrics_sink
            summary["metrics_format"] = metrics_format
            summary["logging_error"] = str(exc)
            summary["error_type"] = "logging_error"
    else:
        summary["metrics_logged"] = False
        summary["metrics_rows_written"] = 0

    return summary


def _default_project_root() -> Path:
    """Resolve repository root from module location."""
    return Path(__file__).resolve().parents[2]


def _validate_inputs(
    df: Any,
    layer: str,
    schema_name: str,
    table_name: str,
    storage_profile: str,
    source_format: str,
    metrics_sink: str,
    metrics_format: str,
    dq_metrics_path: str | None,
) -> dict[str, Any] | None:
    """Validate required function inputs and return a contract error when invalid."""
    for field_name, value in (("layer", layer), ("schema_name", schema_name), ("table_name", table_name)):
        if not isinstance(value, str) or not value.strip():
            return {
                "success": False,
                "error_type": "invalid_input",
                "error_message": f"'{field_name}' must be a non-empty string.",
                "suite_name": None,
                "layer": layer,
                "schema_name": schema_name,
                "table_name": table_name,
            }

    if df is None:
        return {
            "success": False,
            "error_type": "invalid_input",
            "error_message": "'df' must be a Spark DataFrame and cannot be None.",
            "suite_name": build_suite_name(layer, schema_name, table_name),
            "layer": layer,
            "schema_name": schema_name,
            "table_name": table_name,
        }

    if storage_profile not in SUPPORTED_STORAGE_PROFILES:
        return {
            "success": False,
            "error_type": "invalid_input",
            "error_message": (
                f"'storage_profile' must be one of {sorted(SUPPORTED_STORAGE_PROFILES)}."
            ),
            "suite_name": build_suite_name(layer, schema_name, table_name),
            "layer": layer,
            "schema_name": schema_name,
            "table_name": table_name,
        }

    if source_format not in SUPPORTED_SOURCE_FORMATS:
        return {
            "success": False,
            "error_type": "invalid_input",
            "error_message": f"'source_format' must be one of {sorted(SUPPORTED_SOURCE_FORMATS)}.",
            "suite_name": build_suite_name(layer, schema_name, table_name),
            "layer": layer,
            "schema_name": schema_name,
            "table_name": table_name,
        }

    if metrics_sink not in SUPPORTED_METRICS_SINKS:
        return {
            "success": False,
            "error_type": "invalid_input",
            "error_message": f"'metrics_sink' must be one of {sorted(SUPPORTED_METRICS_SINKS)}.",
            "suite_name": build_suite_name(layer, schema_name, table_name),
            "layer": layer,
            "schema_name": schema_name,
            "table_name": table_name,
        }

    if metrics_format not in SUPPORTED_METRICS_FORMATS:
        return {
            "success": False,
            "error_type": "invalid_input",
            "error_message": (
                f"'metrics_format' must be one of {sorted(SUPPORTED_METRICS_FORMATS)}."
            ),
            "suite_name": build_suite_name(layer, schema_name, table_name),
            "layer": layer,
            "schema_name": schema_name,
            "table_name": table_name,
        }

    if metrics_sink == "path" and (dq_metrics_path is None or not dq_metrics_path.strip()):
        return {
            "success": False,
            "error_type": "invalid_input",
            "error_message": "'dq_metrics_path' is required when metrics_sink='path'.",
            "suite_name": build_suite_name(layer, schema_name, table_name),
            "layer": layer,
            "schema_name": schema_name,
            "table_name": table_name,
        }

    return None


def _suite_missing_error(
    layer: str,
    schema_name: str,
    table_name: str,
    suite_name: str,
    suite_path: Path,
) -> dict[str, Any]:
    """Build clear suite-not-found response payload."""
    message = (
        "Expectation suite not found:\n"
        f"{suite_name}\n\n"
        "Expected location:\n"
        f"{suite_path}"
    )
    return {
        "success": False,
        "error_type": "suite_missing",
        "error_message": message,
        "suite_name": suite_name,
        "layer": layer,
        "schema_name": schema_name,
        "table_name": table_name,
    }


def _preflight_environment(df: Any, suite_path: Path) -> str | None:
    """Validate environment dependencies and Spark/GX context assumptions."""
    try:
        importlib.import_module("great_expectations")
    except Exception as exc:
        return (
            "Great Expectations is not available in this environment. "
            "Install project dependencies and attach the correct notebook environment. "
            f"Details: {exc}"
        )

    try:
        importlib.import_module("pyspark")
    except Exception as exc:
        return (
            "PySpark is not available in this environment. "
            "Install project dependencies and ensure Spark runtime is attached. "
            f"Details: {exc}"
        )

    expectations_root = suite_path.parent
    if not expectations_root.exists() or not expectations_root.is_dir():
        return (
            "GX expectations directory is missing. "
            f"Expected directory: {expectations_root}"
        )

    spark_session = getattr(df, "sparkSession", None)
    if spark_session is None:
        return (
            "Input 'df' does not look like a Spark DataFrame because 'sparkSession' "
            "is missing."
        )

    return None


def _validate_suite_yaml(suite_path: Path, expected_suite_name: str) -> str | None:
    """Validate suite YAML structure and suite-name consistency."""
    try:
        yaml = importlib.import_module("yaml")
        with suite_path.open("r", encoding="utf-8") as handle:
            payload = yaml.safe_load(handle) or {}
    except Exception as exc:
        return f"Failed to read suite YAML: {exc}"

    actual_suite_name = payload.get("expectation_suite_name")
    if actual_suite_name and actual_suite_name != expected_suite_name:
        return (
            "Suite naming mismatch. "
            f"Expected '{expected_suite_name}' but YAML declares '{actual_suite_name}'."
        )

    return None


def _run_gx_validation(df: Any, suite_name: str, run_id: str, suite_path: Path) -> dict[str, Any]:
    """Run GX validation for an in-memory Spark DataFrame and return a JSON dict."""
    import great_expectations as gx

    ge_root = suite_path.parents[1]

    get_context = getattr(gx, "get_context", None)
    if get_context is None:
        raise RuntimeError("Great Expectations get_context API is unavailable")

    try:
        context = get_context(context_root_dir=str(ge_root))
    except TypeError:
        context = get_context(project_root_dir=str(ge_root))

    validator = _get_validator_with_fallback(context, df, suite_name, run_id)
    result = validator.validate(result_format="SUMMARY")

    if hasattr(result, "to_json_dict"):
        return result.to_json_dict()
    if isinstance(result, dict):
        return result
    raise RuntimeError("Unexpected validation result object returned by Great Expectations")


def _get_validator_with_fallback(context: Any, df: Any, suite_name: str, run_id: str) -> Any:
    """Build a validator, preferring GX fluent datasources with legacy fallback."""
    try:
        return _get_validator_fluent(context, df, suite_name)
    except Exception:
        return _get_validator_legacy(context, df, suite_name, run_id)


def _get_validator_fluent(context: Any, df: Any, suite_name: str) -> Any:
    """Use fluent Spark datasource API used by modern GX contexts."""
    datasource_name = "spark_runtime"
    asset_name = suite_name
    batch_definition_name = "runtime_whole_dataframe"

    data_sources = getattr(context, "data_sources", None)
    if data_sources is None:
        raise RuntimeError("GX fluent data_sources API is unavailable")

    try:
        spark_ds = data_sources.get(datasource_name)
    except Exception:
        spark_ds = data_sources.add_spark(name=datasource_name)

    assets_by_name = {
        getattr(asset, "name", ""): asset
        for asset in (getattr(spark_ds, "assets", []) or [])
        if getattr(asset, "name", None)
    }
    if asset_name in assets_by_name:
        data_asset = assets_by_name[asset_name]
    else:
        data_asset = spark_ds.add_dataframe_asset(name=asset_name)

    batch_defs_by_name = {
        getattr(batch_def, "name", ""): batch_def
        for batch_def in (getattr(data_asset, "batch_definitions", []) or [])
        if getattr(batch_def, "name", None)
    }
    if batch_definition_name in batch_defs_by_name:
        batch_definition = batch_defs_by_name[batch_definition_name]
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


def _get_validator_legacy(context: Any, df: Any, suite_name: str, run_id: str) -> Any:
    """Fallback for older GX contexts using RuntimeBatchRequest."""
    ge_batch_module = importlib.import_module("great_expectations.core.batch")
    RuntimeBatchRequest = ge_batch_module.RuntimeBatchRequest

    datasource_name = "spark_runtime"
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
                    "batch_identifiers": ["run_id"],
                }
            },
        )

    batch_request = RuntimeBatchRequest(
        datasource_name=datasource_name,
        data_connector_name=data_connector_name,
        data_asset_name=suite_name,
        runtime_parameters={"batch_data": df},
        batch_identifiers={"run_id": run_id},
    )
    return context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=suite_name,
    )


def _write_metrics(
    spark_session: Any,
    metric_rows: list[dict[str, Any]],
    table_name: str,
    metrics_sink: str,
    metrics_format: str,
    dq_metrics_path: str | None,
) -> int:
    """Append metric rows to a table or a path sink using Delta or Parquet."""
    if not metric_rows:
        return 0

    if metrics_sink == "path" and metrics_format == "delta":
        if dq_metrics_path is None:
            raise ValueError("'dq_metrics_path' is required when metrics_sink='path'.")
        try:
            import pyarrow as pa
            from deltalake import write_deltalake
        except Exception as exc:
            raise RuntimeError(
                "Delta path logging requires optional dependencies 'deltalake' and 'pyarrow'."
            ) from exc

        schema = pa.schema(
            [
                ("run_id", pa.string()),
                ("run_ts", pa.timestamp("us", tz="UTC")),
                ("layer", pa.string()),
                ("schema_name", pa.string()),
                ("table_name", pa.string()),
                ("suite_name", pa.string()),
                ("expectation_type", pa.string()),
                ("success", pa.bool_()),
                ("unexpected_percent", pa.float64()),
                ("unexpected_count", pa.int64()),
                ("element_count", pa.int64()),
                ("sampling_strategy", pa.string()),
                ("original_row_count", pa.int64()),
                ("sample_row_count", pa.int64()),
                ("confidence", pa.float64()),
                ("margin_error", pa.float64()),
                ("stratify_by", pa.string()),
                ("details_json", pa.string()),
            ]
        )

        schema_fields = [field.name for field in schema]
        normalized_rows: list[dict[str, Any]] = []
        for row in metric_rows:
            normalized = {name: row.get(name) for name in schema_fields}
            run_ts = normalized.get("run_ts")
            if isinstance(run_ts, str):
                try:
                    normalized["run_ts"] = dt.datetime.fromisoformat(run_ts)
                except ValueError:
                    normalized["run_ts"] = None
            normalized_rows.append(normalized)

        columns = {
            name: [row.get(name) for row in normalized_rows]
            for name in schema_fields
        }
        metrics_table = pa.Table.from_pydict(columns, schema=schema)
        write_deltalake(dq_metrics_path, metrics_table, mode="append")
        return len(metric_rows)

    from pyspark.sql.types import (
        BooleanType,
        DoubleType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    schema = StructType(
        [
            StructField("run_id", StringType(), True),
            StructField("run_ts", TimestampType(), True),
            StructField("layer", StringType(), True),
            StructField("schema_name", StringType(), True),
            StructField("table_name", StringType(), True),
            StructField("suite_name", StringType(), True),
            StructField("expectation_type", StringType(), True),
            StructField("success", BooleanType(), True),
            StructField("unexpected_percent", DoubleType(), True),
            StructField("unexpected_count", LongType(), True),
            StructField("element_count", LongType(), True),
            StructField("sampling_strategy", StringType(), True),
            StructField("original_row_count", LongType(), True),
            StructField("sample_row_count", LongType(), True),
            StructField("confidence", DoubleType(), True),
            StructField("margin_error", DoubleType(), True),
            StructField("stratify_by", StringType(), True),
            StructField("details_json", StringType(), True),
        ]
    )

    schema_fields = [field.name for field in schema.fields]
    normalized_rows: list[dict[str, Any]] = []
    for row in metric_rows:
        normalized = {name: row.get(name) for name in schema_fields}
        run_ts = normalized.get("run_ts")
        if isinstance(run_ts, str):
            try:
                normalized["run_ts"] = dt.datetime.fromisoformat(run_ts)
            except ValueError:
                normalized["run_ts"] = None
        normalized_rows.append(normalized)

    metrics_df = spark_session.createDataFrame(normalized_rows, schema=schema)
    writer = metrics_df.write.format(metrics_format).mode("append")
    if metrics_sink == "table":
        writer.saveAsTable(table_name)
    else:
        if dq_metrics_path is None:
            raise ValueError("'dq_metrics_path' is required when metrics_sink='path'.")
        writer.save(dq_metrics_path)
    return len(metric_rows)
