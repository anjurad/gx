"""Great Expectations validation flow loading tests from a suite YAML file.

This script reads expectations from
`great_expectations/expectations/bronze.sales.green_tripdata_2017.yml`
and applies them to a local Spark DataFrame batch before validating.
"""

from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path
import shutil
import sys
from typing import TYPE_CHECKING

import yaml

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

# -----------------------------
# Local configuration
# -----------------------------
PROJECT_ROOT = Path(__file__).resolve().parent
REPO_ROOT = PROJECT_ROOT.parent
GX_PROJECT_PATH = REPO_ROOT / "gx"
SUITE_FILE_PATH = (
    GX_PROJECT_PATH
    / "expectations"
    / "bronze.sales.green_tripdata_2017.yml"
)
DATASOURCE_NAME = "local_spark"
ASSET_NAME = "green_trips_asset"
BATCH_DEFINITION_NAME = "green_trip_batch_definition"
SOURCE_CSV_PATH = (
    REPO_ROOT / "data" / "green_tripdata_2017_sample.csv"
)
ROW_LIMIT = 1000
ENABLE_DF_CACHE = False


def _ensure_supported_python() -> None:
    """Fail fast on Python versions unsupported by current GX releases.

    Raises:
        RuntimeError: If running on Python 3.14 or newer.
    """
    if sys.version_info >= (3, 14):
        raise RuntimeError(
            "Great Expectations currently has compatibility issues on Python 3.14+. "
            "Use Python 3.10-3.13 for local runs."
        )


def _index_named_objects(items: Iterable[object] | None) -> dict[str, object]:
    """Build a name-to-object index for items that expose a `.name` attribute."""
    indexed: dict[str, object] = {}
    for item in items or []:
        name = getattr(item, "name", None)
        if isinstance(name, str) and name:
            indexed[name] = item
    return indexed


def _load_suite_expectations(suite_path: Path) -> tuple[str, list[dict[str, object]]]:
    """Load suite name and expectation entries from a YAML suite file."""
    if not suite_path.exists():
        raise FileNotFoundError(f"Suite file not found at: {suite_path}")

    parsed = yaml.safe_load(suite_path.read_text(encoding="utf-8")) or {}
    suite_name = parsed.get("expectation_suite_name")
    expectations = parsed.get("expectations", [])

    if not isinstance(suite_name, str) or not suite_name:
        raise RuntimeError(f"Missing valid expectation_suite_name in {suite_path}")
    if not isinstance(expectations, list):
        raise RuntimeError(f"Invalid expectations list in {suite_path}")

    normalized: list[dict[str, object]] = []
    for item in expectations:
        if not isinstance(item, dict):
            raise RuntimeError(f"Invalid expectation entry in {suite_path}: {item!r}")
        expectation_type = item.get("expectation_type")
        kwargs = item.get("kwargs", {})
        if not isinstance(expectation_type, str) or not expectation_type:
            raise RuntimeError(f"Invalid expectation_type in {suite_path}: {item!r}")
        if not isinstance(kwargs, dict):
            raise RuntimeError(f"Invalid kwargs for {expectation_type} in {suite_path}")
        normalized.append({"expectation_type": expectation_type, "kwargs": kwargs})

    return suite_name, normalized


def _get_spark_session() -> object:
    """Return a Spark session suitable for local execution."""
    if shutil.which("java") is None:
        raise RuntimeError(
            "Java runtime not found. Install a JRE/JDK (for example OpenJDK 21) "
            "before running PySpark locally."
        )

    try:
        from pyspark.sql import SparkSession

        return (
            SparkSession.getActiveSession()
            or SparkSession.builder.appName("gx-local-validation").master("local[*]").getOrCreate()
        )
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "PySpark is not installed in this environment. Install dependencies from pyproject.toml "
            "and retry."
        ) from exc
    except Exception as exc:  # pragma: no cover - runtime environment dependent
        raise RuntimeError(
            "Could not initialize local SparkSession. Ensure Java is installed and "
            "JAVA_HOME is configured if required by your environment."
        ) from exc


def _load_sample_dataframe(spark: object) -> object:
    """Load the sample CSV into a Spark DataFrame."""
    source_path = SOURCE_CSV_PATH
    if not source_path.exists():
        raise FileNotFoundError(f"Sample dataset not found at: {source_path}")

    df = spark.read.option("header", True).option("inferSchema", True).csv(str(source_path))
    return df.limit(ROW_LIMIT)


def main() -> None:
    """Run GX validation using expectations imported from a YAML suite file."""
    _ensure_supported_python()
    import great_expectations as gx

    suite_name, expected_tests = _load_suite_expectations(SUITE_FILE_PATH)

    spark = _get_spark_session()

    try:
        context = gx.get_context(
            mode="file",
            project_root_dir=str(REPO_ROOT),
        )
    except Exception as exc:  # pragma: no cover - environment/storage dependent
        raise RuntimeError(
            f"Failed to create GX file context under {REPO_ROOT}."
        ) from exc

    print(f"GX Context Root: {context.root_directory}")

    data_sources_by_name = _index_named_objects(context.data_sources.all())
    if DATASOURCE_NAME in data_sources_by_name:
        spark_ds = context.data_sources.get(DATASOURCE_NAME)
        print(f"Retrieved existing datasource: {DATASOURCE_NAME}")
    else:
        try:
            spark_ds = context.data_sources.add_spark(name=DATASOURCE_NAME)
            print(f"Created datasource: {DATASOURCE_NAME}")
        except Exception as exc:
            if "already exists" not in str(exc):
                raise
            spark_ds = context.data_sources.get(DATASOURCE_NAME)
            print(f"Retrieved existing datasource: {DATASOURCE_NAME}")

    assets_by_name = _index_named_objects(getattr(spark_ds, "assets", []) or [])
    if ASSET_NAME in assets_by_name:
        data_asset = assets_by_name[ASSET_NAME]
        print(f"Retrieved existing asset: {ASSET_NAME}")
    else:
        data_asset = spark_ds.add_dataframe_asset(name=ASSET_NAME)
        print(f"Created asset: {ASSET_NAME}")

    suites_by_name = _index_named_objects(context.suites.all())
    if suite_name not in suites_by_name:
        context.suites.add(gx.ExpectationSuite(name=suite_name))
        print(f"Created suite: {suite_name}")

    df = _load_sample_dataframe(spark)
    if ENABLE_DF_CACHE:
        df = df.cache()

    batch_defs_by_name = _index_named_objects(
        getattr(data_asset, "batch_definitions", []) or []
    )
    if BATCH_DEFINITION_NAME in batch_defs_by_name:
        batch_definition = batch_defs_by_name[BATCH_DEFINITION_NAME]
        print(f"Retrieved existing batch definition: {BATCH_DEFINITION_NAME}")
    else:
        batch_definition = data_asset.add_batch_definition_whole_dataframe(
            name=BATCH_DEFINITION_NAME
        )
        print(f"Created batch definition: {BATCH_DEFINITION_NAME}")

    batch_request = batch_definition.build_batch_request(
        batch_parameters={"dataframe": df}
    )
    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=suite_name,
    )
    print(f"Validator ready for suite: {suite_name}")

    applied_count = 0
    for test in expected_tests:
        expectation_type = str(test["expectation_type"])
        kwargs = dict(test["kwargs"])

        expectation_fn = getattr(validator, expectation_type, None)
        if expectation_fn is None or not callable(expectation_fn):
            raise RuntimeError(
                f"Expectation '{expectation_type}' is not supported by this validator."
            )

        expectation_fn(**kwargs)
        applied_count += 1

    print(f"Applied {applied_count} expectations from {SUITE_FILE_PATH.name}")

    try:
        results = validator.validate()
    except Exception as exc:  # pragma: no cover - execution/data dependent
        raise RuntimeError(
            "GX validation execution failed. Verify Spark setup and suite configuration."
        ) from exc

    context.suites.add_or_update(validator.get_expectation_suite())

    print(f"Validation success: {results.success}")
    print(f"Validation statistics: {results.statistics}")


if __name__ == "__main__":
    main()
