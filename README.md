# Great Expectations + PySpark Lakehouse Data Quality Framework

This repository contains a convention-driven DQ framework for Microsoft Fabric Spark.

## Conventions

- Expectation suite name: `{layer}.{schema}.{table}`
- Suite file path: `gx/expectations/{layer}.{schema}.{table}.yml`
- No suite scanning and no metadata registry lookup

## Core Contract

`dq.run_data_quality` (importable from notebooks or Python modules):

```python
from dq import run_data_quality

run_data_quality(
    df,
    layer: str,
    schema_name: str,
    table_name: str,
    sampling_config: dict | None = None,
    log_metrics: bool = True,
    dq_metrics_table: str = "analytics.dq_metrics",
    dq_metrics_path: str | None = None,
    storage_profile: str = "local_fs",  # local_fs | azurite_blob
    source_format: str = "csv",         # csv | parquet | delta
    metrics_sink: str = "table",        # table | path
    metrics_format: str = "delta",      # delta | parquet
) -> dict
```

The wrapper derives suite names deterministically and does not require callers to
pass a suite name.

## Process Flow

The diagram below shows the active wrapper execution path implemented in `src/dq/`,
plus the GX project artefacts in `gx/` that supply configuration, expectations,
and generated outputs.

```mermaid
flowchart LR
    subgraph Runtime[Active runtime flow]
        A[Caller input\nSpark DataFrame df\nlayer, schema_name, table_name\noptional sampling and metrics config]
        B[run_data_quality\nsrc/dq/dq_runner.py]
        C[Deterministic suite lookup\nbuild_suite_name + resolve_suite_path\ngx/expectations/{layer}.{schema}.{table}.yml]
        D[Preflight checks\ninput contract\nenvironment readiness\nsuite YAML validity]
        E[Sampling\nsrc/dq/sampling.py\nfull | statistical | stratified | auto]
        F[GX validation\n_run_gx_validation\nvalidate sampled DataFrame against suite]
        G[Normalise results\nsrc/dq/normalise.py\nGX result -> metric rows + summary]
        H[Optional metrics write\n_write_metrics\ntable: analytics.dq_metrics\nor path: Delta/Parquet]
        I[Returned summary dict\nsuccess, expectation counts\nsampling metadata\nmetrics logging status]

        A --> B --> C --> D --> E --> F --> G --> H --> I
        G --> I
    end

    subgraph GX[GX project artefacts]
        J[gx/great_expectations.yml\nGX context config\nstores, fluent datasources\ndata docs site, plugins directory]
        K[gx/expectations/*.yml\nExpectation suites\nrequired validation input artefact]
        L[gx/uncommitted/validations/\nvalidation result JSON artefacts]
        M[gx/uncommitted/data_docs/\nrendered GX Data Docs output]
        N[gx/plugins/custom_data_docs/\ncustom docs styling and renderers]
        O[gx/checkpoints/\nconfigured store\nnot used by current wrapper flow]
        P[gx/validation_definitions/\nconfigured store\nnot used by current wrapper flow]
    end

    J --> F
    K --> C
    F --> L
    L --> M
    N --> M
    O -. optional GX orchestration .-> F
    P -. optional GX orchestration .-> F
```

Active execution today is the `run_data_quality` path in `src/dq/`: caller input,
suite resolution, preflight validation, sampling, GX validation, normalization,
optional metrics persistence, and the returned summary. The `gx/checkpoints/` and
`gx/validation_definitions/` artefacts are part of the GX project structure and
stores configured in `gx/great_expectations.yml`, but they are not invoked by the
current wrapper implementation.

## Environment Readiness

The function validates that the runtime environment can support deterministic suite
execution before running GX validation. Preflight checks include:

- Great Expectations import availability
- PySpark import availability
- Spark DataFrame compatibility (`df.sparkSession`)
- Expectations directory availability under `gx/expectations`

If a suite is missing, the wrapper returns a clear error with the derived suite
name and expected file location:

```text
Expectation suite not found:
bronze.nyc_taxi.green_trips

Expected location:
.../gx/expectations/bronze.nyc_taxi.green_trips.yml
```

## Fabric Usage

1. Attach an Environment with required libraries (including Great Expectations).
2. If you enable metrics logging to a table, provision `analytics.dq_metrics` in your Fabric environment.
3. In a notebook, import and call the wrapper directly:

```python
from dq import run_data_quality

summary = run_data_quality(
    df=df,
    layer="bronze",
    schema_name="nyc_taxi",
    table_name="green_trips",
)
```

## Sampling Modes

- `full`: full dataset scan
- `statistical`: Cochran + finite population correction
- `stratified`: `sampleBy` by selected column
- `auto`: stratified when `stratify_by` is provided, otherwise statistical

When computed sample size is greater than or equal to population size, full scan is used.

## Pipeline Stability Notes (Fabric)

- Prefer Environment-managed libraries for production pipelines.
- Avoid inline `%pip` in referenced notebooks for pipeline runs.
- Keep table notebooks thin and orchestration-focused.

## Local Notebook Setup

Install notebook dependencies (including `ipykernel`) from `pyproject.toml`:

```bash
source .venv/bin/activate
uv sync --extra notebook
```

Register a reusable kernel for this environment:

```bash
source .venv/bin/activate
python -m ipykernel install --user --name great-expectations --display-name "Python (.venv) great-expectations"
```

Then select the `Python (.venv) great-expectations` kernel in VS Code/Jupyter notebooks.

## Local Run (Sample Dataset)

Use `src/optimised-04.py` to run wrapper-first GX validation locally against
`data/green_tripdata_2017_sample.csv`.

Prerequisites:

- Python `3.10` to `3.13` (do not use `3.14+` for current GX versions)
- Java runtime for PySpark (Debian example: `openjdk-21-jre-headless`)

Tested setup and run commands:

```bash
# 1) Install Java (Debian trixie)
sudo apt-get update
sudo apt-get install -y openjdk-21-jre-headless

# 2) Create the project virtual environment
uv venv

# 3) Install project dependencies
source .venv/bin/activate
uv sync --extra notebook

# 4) Run local GX validation via the wrapper API
python src/optimised-04.py
```
