"""Great Expectations validation flow for local Spark execution.

This script uses a project-local GX File Data Context and idempotent
get-or-create patterns for datasource, data asset, batch definition, and suite.
It is configured to run against the repository sample CSV dataset.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from pathlib import Path
import shutil
import sys
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import great_expectations as gx
    from pyspark.sql import DataFrame, SparkSession

# -----------------------------
# Local configuration
# -----------------------------
PROJECT_ROOT = Path(__file__).resolve().parent
REPO_ROOT = PROJECT_ROOT.parent
LAKEHOUSE_GX_PROJECT_PATH = str(REPO_ROOT / ".gx")
DATASOURCE_NAME = "local_spark"
ASSET_NAME = "green_trips_asset"
BATCH_DEFINITION_NAME = "green_trip_batch_definition"
SUITE_NAME = "green_trip_suite"
SOURCE_CSV_PATH = str(
    REPO_ROOT / "local_mvp" / "data" / "green_tripdata_2017_sample.csv"
)
ROW_LIMIT = 1000
VALIDATION_COLUMN = "vendorID"

# Optional performance controls for larger runs.
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
    """Build a name-to-object index for items that expose a `.name` attribute.

    Args:
        items: Iterable of GX objects where each object may have a `name`.

    Returns:
        A dictionary keyed by object name.
    """
    indexed: dict[str, object] = {}
    for item in items or []:
        name = getattr(item, "name", None)
        if isinstance(name, str) and name:
            indexed[name] = item
    return indexed


def _expectation_exists(
    suite: object,
    expectation_type: str,
    expected_kwargs: Mapping[str, object],
) -> bool:
    """Check whether an expectation exists with exact type and kwargs match.

    Args:
        suite: Expectation suite to inspect.
        expectation_type: GX expectation method/type string.
        expected_kwargs: Expected kwargs for the expectation.

    Returns:
        True if an expectation with an exact kwargs match exists; else False.
    """
    for expectation in suite.expectations or []:
        existing_type = getattr(expectation, "type", None)
        existing_kwargs = dict(getattr(expectation, "kwargs", {}) or {})
        if existing_type == expectation_type and existing_kwargs == dict(expected_kwargs):
            return True
    return False


def _get_spark_session() -> object:
    """Return a Spark session suitable for local execution.

    Returns:
        The active SparkSession.

    Raises:
        RuntimeError: If a Spark session cannot be established.
    """
    if shutil.which("java") is None:
        raise RuntimeError(
            "Java runtime not found. Install a JRE/JDK (for example OpenJDK 17) "
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
    """Load the sample CSV into a Spark DataFrame.

    Args:
        spark: Active SparkSession.

    Returns:
        Spark DataFrame loaded from sample CSV.

    Raises:
        FileNotFoundError: If the sample CSV path does not exist.
        RuntimeError: If the validation column is missing from the dataframe.
    """
    source_path = Path(SOURCE_CSV_PATH)
    if not source_path.exists():
        raise FileNotFoundError(f"Sample dataset not found at: {source_path}")

    df = spark.read.option("header", True).option("inferSchema", True).csv(str(source_path))
    selected_df = df.select(VALIDATION_COLUMN).limit(ROW_LIMIT)

    if VALIDATION_COLUMN not in selected_df.columns:
        raise RuntimeError(
            f"Validation column '{VALIDATION_COLUMN}' not found in sample dataset at {source_path}"
        )

    return selected_df


def main() -> None:
    """Run idempotent GX validation for a local Spark dataframe batch."""
    _ensure_supported_python()
    import great_expectations as gx

    spark = _get_spark_session()

    Path(LAKEHOUSE_GX_PROJECT_PATH).mkdir(parents=True, exist_ok=True)

    try:
        context = gx.get_context(
            mode="file",
            project_root_dir=LAKEHOUSE_GX_PROJECT_PATH,
        )
    except Exception as exc:  # pragma: no cover - environment/storage dependent
        raise RuntimeError(
            "Failed to create GX file context at "
            f"{LAKEHOUSE_GX_PROJECT_PATH}."
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
            # Defensive fallback: some GX contexts can report an empty `all()` list
            # while still containing the datasource in the fluent store.
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
    if SUITE_NAME in suites_by_name:
        _ = suites_by_name[SUITE_NAME]
        print(f"Retrieved existing suite: {SUITE_NAME}")
    else:
        context.suites.add(gx.ExpectationSuite(name=SUITE_NAME))
        print(f"Created suite: {SUITE_NAME}")

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
        expectation_suite_name=SUITE_NAME,
    )
    print("Validator ready")

    current_suite = validator.get_expectation_suite()

    column_exists_kwargs: dict[str, object] = {"column": VALIDATION_COLUMN}
    not_null_kwargs: dict[str, object] = {"column": VALIDATION_COLUMN}

    if not _expectation_exists(
        current_suite,
        "expect_column_to_exist",
        column_exists_kwargs,
    ):
        validator.expect_column_to_exist(VALIDATION_COLUMN)

    if not _expectation_exists(
        current_suite,
        "expect_column_values_to_not_be_null",
        not_null_kwargs,
    ):
        validator.expect_column_values_to_not_be_null(VALIDATION_COLUMN)

    try:
        results = validator.validate()
    except Exception as exc:  # pragma: no cover - execution/data dependent
        raise RuntimeError(
            "GX validation execution failed. Verify Spark setup and expectation configuration."
        ) from exc

    updated_suite = validator.get_expectation_suite()
    context.suites.add_or_update(updated_suite)

    print(f"Validation success: {results.success}")
    print(f"Validation statistics: {results.statistics}")
    print("Suite updated successfully.")


if __name__ == "__main__":
    main()
