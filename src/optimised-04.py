"""Wrapper-first local runner for deterministic GX data quality checks.

This script demonstrates the canonical flow:
1) load a local Spark DataFrame,
2) pass dataset coordinates,
3) let `dq.run_data_quality` resolve and run the matching suite.
"""

from __future__ import annotations

from pathlib import Path
import shutil
import sys
from typing import Any

from dq import run_data_quality
from dq.dq_runner import build_suite_name, resolve_suite_path

# -----------------------------
# Local configuration
# -----------------------------
PROJECT_ROOT = Path(__file__).resolve().parent
REPO_ROOT = PROJECT_ROOT.parent

SOURCE_CSV_PATH = REPO_ROOT / "data" / "green_tripdata_2017_sample.csv"
SOURCE_PARQUET_PATH = REPO_ROOT / "data" / "green_tripdata_2017_sample.parquet"
SOURCE_DELTA_PATH = REPO_ROOT / "data" / "green_tripdata_2017_sample.delta"
SOURCE_FORMAT = "csv"
ROW_LIMIT = 1000
ENABLE_DF_CACHE = False

LAYER = "bronze"
SCHEMA_NAME = "sales"
TABLE_NAME = "green_tripdata_2017"

LOG_METRICS = False
DQ_METRICS_TABLE = "analytics.dq_metrics"
DQ_METRICS_PATH = None
METRICS_SINK = "table"
METRICS_FORMAT = "delta"
STORAGE_PROFILE = "local_fs"


def _ensure_supported_python() -> None:
    """Fail fast on Python versions unsupported by current GX releases."""
    if sys.version_info >= (3, 14):
        raise RuntimeError(
            "Great Expectations currently has compatibility issues on Python 3.14+. "
            "Use Python 3.10-3.13 for local runs."
        )


def _get_spark_session() -> Any:
    """Return a Spark session suitable for local execution."""
    if shutil.which("java") is None:
        raise RuntimeError(
            "Java runtime not found. Install a JRE/JDK (for example OpenJDK 21) "
            "before running PySpark locally."
        )

    try:
        from pyspark.sql import SparkSession

        builder = getattr(SparkSession, "builder")
        return (
            SparkSession.getActiveSession()
            or builder.appName("dq-wrapper-local-validation")
            .master("local[*]")
            .getOrCreate()
        )
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "PySpark is not installed in this environment. Install dependencies from "
            "pyproject.toml and retry."
        ) from exc
    except Exception as exc:  # pragma: no cover - runtime environment dependent
        raise RuntimeError(
            "Could not initialize local SparkSession. Ensure Java is installed and "
            "JAVA_HOME is configured if required by your environment."
        ) from exc


def _load_sample_dataframe(spark: Any) -> Any:
    """Load sample data into a Spark DataFrame using the configured source format."""
    source_paths = {
        "csv": SOURCE_CSV_PATH,
        "parquet": SOURCE_PARQUET_PATH,
        "delta": SOURCE_DELTA_PATH,
    }
    source_path = source_paths.get(SOURCE_FORMAT)
    if source_path is None:
        raise ValueError("SOURCE_FORMAT must be one of: csv, parquet, delta")
    if not source_path.exists():
        raise FileNotFoundError(f"Sample dataset not found at: {source_path}")

    reader = spark.read
    if SOURCE_FORMAT == "csv":
        reader = reader.option("header", True).option("inferSchema", True)
    df = reader.format(SOURCE_FORMAT).load(str(source_path))
    df = df.limit(ROW_LIMIT)
    if ENABLE_DF_CACHE:
        df = df.cache()
    return df


def _print_result(result: dict[str, Any], suite_name: str, suite_path: Path) -> None:
    """Print a concise human-readable result summary."""
    print(f"Derived suite name: {suite_name}")
    print(f"Expected suite path: {suite_path}")

    if not result.get("success", False):
        print("Validation success: False")
        print(f"Error type: {result.get('error_type', 'unknown_error')}")
        print(f"Error message:\n{result.get('error_message', 'No details provided.')}")
        return

    print("Validation success: True")
    print(f"Run ID: {result.get('run_id')}")
    print(f"Evaluated expectations: {result.get('evaluated_expectations')}")
    print(f"Successful expectations: {result.get('successful_expectations')}")
    print(f"Failed expectations: {result.get('failed_expectations')}")
    print(f"Sampling strategy: {result.get('sampling_strategy')}")


def main() -> None:
    """Execute a wrapper-first local DQ run."""
    _ensure_supported_python()

    spark = _get_spark_session()
    df = _load_sample_dataframe(spark)

    suite_name = build_suite_name(LAYER, SCHEMA_NAME, TABLE_NAME)
    suite_path = resolve_suite_path(
        LAYER,
        SCHEMA_NAME,
        TABLE_NAME,
        project_root=REPO_ROOT,
    )

    result = run_data_quality(
        df=df,
        layer=LAYER,
        schema_name=SCHEMA_NAME,
        table_name=TABLE_NAME,
        log_metrics=LOG_METRICS,
        dq_metrics_table=DQ_METRICS_TABLE,
        dq_metrics_path=DQ_METRICS_PATH,
        storage_profile=STORAGE_PROFILE,
        source_format=SOURCE_FORMAT,
        metrics_sink=METRICS_SINK,
        metrics_format=METRICS_FORMAT,
        project_root=REPO_ROOT,
    )

    _print_result(result, suite_name, suite_path)

    if not result.get("success", False):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
