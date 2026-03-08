# Great Expectations Data Quality Solution Summary

## Purpose of This Document

This document provides a business-oriented summary of the current data quality solution implemented in this repository. It is intended as a handoff artifact for a business analyst who needs to create a formal specification, including user stories, requirements, acceptance criteria, and supporting analysis.

This is not a specification. It is a structured description of what the current solution does, how it is used, what decisions are already embedded in the implementation, and what areas are likely to need formal business treatment.

## Solution Overview

The current solution is a lightweight data quality validation framework built on top of Great Expectations and PySpark. It is designed to validate Spark DataFrames or Spark tables against predefined expectation suites, return a simplified validation result, and optionally persist both human-readable outputs and expectation-level metrics for historical analysis.

The active implementation is the `gx_framework` package under `src/gx_framework`. This is the supported path in the repository. An older wrapper-based implementation exists under `archive/legacy-dq`, but it is historical and should not be treated as the primary product path.

At a business level, the solution enables teams to:

- run data quality checks against known datasets
- map business datasets to maintained validation rule sets
- get a simplified pass/fail-oriented output for operational use
- retain validation evidence for audit or troubleshooting purposes
- optionally retain expectation-level metrics over time for trend analysis
- validate either direct in-memory data or named Spark tables using the same contract

## Business Problem the Solution Addresses

The solution addresses the need for a repeatable, standardized way to apply data quality controls to Spark-based datasets. It reduces the amount of Great Expectations setup knowledge required by the caller and provides a simplified interface for validation execution.

In practical terms, the solution is intended to solve these business needs:

- a dataset should be validated against a known rule set before downstream use
- validation should be easy enough to invoke from notebooks or orchestration code
- dataset-to-rule mapping should be centrally controlled rather than redefined per notebook
- the result should be understandable without requiring a deep understanding of Great Expectations internals
- validation outcomes and evidence should be retained when needed
- failed validations should expose enough detail to support operational follow-up

## Current Product Boundary

The current solution is focused on runtime validation execution. It is not a full data quality platform.

The current boundary includes:

- configuration-driven validation execution
- expectation suite resolution
- Great Expectations context loading and validation execution
- simplified result shaping
- JSON log and result persistence
- optional Delta-based expectation metrics persistence
- optional row sampling for large datasets
- notebook-based demonstration and usage

The current boundary does not include a full user interface, workflow approval process, stewardship workflow, rules authoring experience, alerting workflow, issue management workflow, or enterprise policy management layer.

## Primary Users and Likely Stakeholders

Based on the implementation, the most likely primary users and stakeholders are:

- data engineers who run validations in pipelines or notebooks
- analytics engineers or platform engineers who maintain validation mappings and defaults
- data quality owners who define or review expected dataset rules
- operational users who need a simple success or failure summary
- technical stakeholders interested in retained validation evidence and metrics history

For future specification work, the BA will likely want to distinguish between at least these personas:

- validation runner: invokes validation for a dataset
- framework maintainer: manages config defaults and dataset mappings
- rule author: maintains expectation suites in Great Expectations
- consumer of results: reviews run outcome and failure details
- governance or audit stakeholder: needs retained run evidence and traceability

## Supported Business Capabilities

### 1. Standardized Validation Entry Points

The solution exposes two primary validation modes:

- validate a Spark DataFrame directly
- validate a Spark table by table name

This means the solution supports both ad hoc notebook-style validation and integration with environments where data is already registered in Spark.

### 2. Dataset-to-Suite Resolution

Validation can be driven by either an explicit expectation suite name or a logical dataset name. When only a dataset name is supplied, the framework resolves the expectation suite using built-in precedence rules.

The resolution path currently supports:

- explicit suite name provided by caller
- dataset name plus a default suite suffix
- dataset mapping from a configuration file
- plain dataset name as a final fallback

This is a key business capability because it separates the identity of a business dataset from the physical or technical validation rule artifact.

### 3. Centralized Runtime Configuration

The framework loads defaults from a central configuration file. These defaults control runtime behavior such as locations, logging, result persistence, metrics persistence, validation result format, and sampling behavior.

This gives the current solution a configuration-first operating model rather than a notebook-first model.

### 4. Use of Managed Expectation Suites

The actual validation rules are stored as Great Expectations suites under the repository’s GX project structure. These suites represent the current rule definitions for supported datasets.

The solution currently includes suites for sample green taxi datasets and demonstrates both YAML and JSON suite artifact forms.

### 5. Simplified Validation Result Contract

Instead of returning the full raw Great Expectations payload as the main product interface, the framework produces a simplified JSON-serializable summary.

The current result includes:

- overall success or failure
- dataset name
- resolved suite name
- run name
- count of total expectations evaluated
- count of successful expectations
- count of failed expectations
- success percentage
- validation timestamp
- original row count
- validated row count
- sampling metadata
- failure details at expectation level

This is a strong indicator that the intended product contract is the simplified result, not the raw GX object model.

### 6. Failure Detail Exposure

When expectations fail, the output includes a concise set of failure details. This currently captures:

- expectation type
- related column where applicable
- unexpected count where available

This provides enough operational detail for first-level diagnosis without forcing users to parse the full Great Expectations response.

### 7. Structured Logging and Run Evidence

The framework writes structured JSON log events and can persist per-run validation result files. This supports traceability, troubleshooting, and evidence retention.

Persisted outputs currently include:

- daily JSON-formatted log files
- per-run JSON result files
- Great Expectations validation artifacts and Data Docs under the GX project structure

### 8. Optional Expectation-Level Metrics Retention

When enabled, the solution flattens validation output into one metrics row per expectation and appends those rows to a Delta table.

This enables use cases such as:

- tracking failure patterns over time
- trending specific expectations
- measuring success rates by dataset or rule type
- retaining historical evidence beyond the latest run result

This capability appears aimed at analytical follow-up rather than just operational execution.

### 9. Optional Sampling for Large Datasets

The framework can validate a sample rather than the full dataset. This is configurable and can operate in different modes.

Supported sampling concepts currently include:

- full validation
- statistical sampling
- stratified sampling
- auto mode that resolves the sampling approach based on configuration

This indicates a business tradeoff already exists in the design between validation completeness and runtime cost.

### 10. Notebook-Centric Demonstration Path

The primary usage demonstration is a notebook that shows how the framework is expected to be consumed. The notebook acts as the practical acceptance path for the current implementation and demonstrates both direct DataFrame validation and Spark table validation.

## Current End-to-End Business Flow

At a high level, the current solution flow is:

1. A caller supplies a Spark DataFrame or a Spark table name.
2. The caller supplies either a logical dataset name or an explicit suite name.
3. The framework loads runtime defaults.
4. The framework resolves the appropriate expectation suite.
5. The framework confirms that the suite artifact exists in the GX project.
6. The framework optionally applies sampling.
7. The framework initializes the Great Expectations context.
8. The framework prepares a runtime validator for the supplied data.
9. The framework executes validation in Great Expectations.
10. The framework transforms the GX output into a simplified result contract.
11. The framework optionally writes logs, a result file, and expectation-level Delta metrics.
12. The simplified result is returned to the caller.

## Current Inputs

The current solution expects the following categories of inputs.

### Caller Inputs

- Spark DataFrame or Spark table name
- logical dataset name or explicit suite name
- optional run name
- optional save-results flag
- optional fail-on-error flag
- optional log level override
- optional sampling overrides
- optional path to a configuration file

### Repository-Managed Inputs

- framework defaults configuration
- dataset-to-suite mapping configuration
- Great Expectations project configuration
- expectation suites

### Platform Dependencies

- Python runtime
- PySpark runtime and active Spark session for table-based validation
- Great Expectations library
- optional Delta Lake and PyArrow libraries for metrics persistence

## Current Outputs

The current outputs fall into two categories: synchronous outputs returned to the caller and persisted outputs written to storage.

### Returned Output

The returned output is a simplified validation summary suitable for downstream programmatic use.

### Persisted Output

Depending on configuration, the solution can produce:

- JSON log files in the logs area
- JSON validation result files for individual runs
- Delta-based metrics history for each expectation evaluated
- Great Expectations validation artifacts and Data Docs in the GX project structure

## Current Business Rules Embedded in the Implementation

The following decisions are already embedded in code and should likely be treated as candidate business or solution rules when a formal specification is written.

### Validation Invocation Rules

- the system supports DataFrame-based validation and table-based validation
- table validation depends on an active Spark session
- validation may be executed using a dataset name, explicit suite name, or both

### Suite Resolution Rules

- an explicit suite name takes precedence over derived or mapped values
- a dataset name can resolve to a suite by suffix convention
- a dataset can resolve to a suite through centralized mapping
- if the preferred resolution target does not physically exist, the framework attempts fallback candidates
- if no valid suite artifact can be found, validation fails

### Result Rules

- the returned result is intentionally simplified
- failure details are retained in a compact form rather than exposing the entire raw GX response as the main contract
- success percentage and expectation counts are part of the standard output

### Persistence Rules

- result-file persistence is configurable
- metrics persistence is configurable and currently supports Delta format only
- metrics persistence may be either best-effort or mandatory depending on configuration
- when metrics persistence is best-effort, validation can still succeed even if metrics writing fails
- when metrics persistence is mandatory, metrics write failure causes validation failure

### Error Handling Rules

- validation failure can either return a failed result or raise an exception depending on flags and defaults
- critical execution issues such as missing Spark session, missing GX context, or missing suite artifact are surfaced as errors

### Sampling Rules

- sampling is optional
- full validation is the default behavior
- sampling configuration can be defined centrally and overridden at run time
- auto mode selects a sampling strategy based on whether stratification is configured

## Current Configuration Model

The current solution uses a small set of configuration assets that act as key control points.

### Framework Defaults

The validation defaults file controls:

- GX root path
- logs path
- default datasource name
- naming suffixes
- whether results are saved
- whether metrics logging is enabled
- metrics storage path and behavior
- validation failure behavior
- logging level
- sampling defaults
- output format

### Dataset Mapping

The dataset mapping file links logical dataset names to expectation suite names. This is an important business-facing control because it determines which rule set applies to which dataset without requiring callers to know internal GX suite names.

### Metrics Demo Configuration

There is also a dedicated example configuration for notebook demonstrations where metrics persistence is enabled. This suggests the solution already anticipates different operating modes, such as a default lightweight mode and a more analytical metrics-enabled mode.

## Current Solution Architecture

At a conceptual level, the solution is composed of the following architectural responsibilities.

### Public Validation Layer

Provides the callable interface for notebook or programmatic use.

### Configuration Layer

Loads and normalizes defaults and dataset mappings.

### Suite Resolution Layer

Determines which expectation suite should be used for a given validation run.

### GX Context and Execution Layer

Loads the Great Expectations context and prepares a runtime validator.

The implementation currently prefers the modern GX fluent datasource approach but includes a fallback to the older runtime batch request approach for compatibility.

### Result Normalization Layer

Converts raw GX results into a stable, simpler business-facing contract.

### Persistence Layer

Handles run result storage, structured logging, and optional Delta metrics logging.

### Sampling Layer

Determines whether the full dataset or a sample is validated.

## Current Assets in Scope

The following repository assets are part of the active solution footprint.

- active framework code under `src/gx_framework`
- runtime config under `config`
- GX project and expectation suites under `gx`
- logs and stored outputs under `logs`
- primary demonstration notebook under `notebooks`
- automated tests under `tests`

The archived legacy implementation under `archive/legacy-dq` should be treated as historical reference material, not the primary product definition.

## Operational Characteristics

From the current implementation, the solution appears intended to be:

- lightweight to invoke from notebooks or pipeline code
- configuration-driven rather than UI-driven
- repository-managed for rule artifacts and defaults
- suitable for local or platform-hosted Spark execution
- usable in a best-effort operational mode or a stricter controlled mode

The implementation also suggests an emphasis on pragmatic usability:

- callers do not need to manually manage Great Expectations internals for each run
- the main output is deliberately compact
- optional persistence behaviors can be turned on only where needed

## Current Limitations and Gaps Relevant for Specification Work

These are not implementation defects by themselves, but they are areas that a BA will likely need to address explicitly in a future specification.

### Functional Gaps

- no business-facing UI is provided
- no formal user roles or permissions model is implemented
- no native alerting or notification workflow is implemented
- no remediation workflow is implemented after failure
- no rule authoring workflow is exposed beyond editing GX suite files
- no approval or promotion workflow exists for rule changes
- no scheduler or orchestration product layer is included in the framework itself

### Governance and Process Gaps

- ownership of dataset mappings is not formally modeled
- ownership of expectation suites is not formally modeled
- no explicit data quality severity model is implemented
- no business glossary or rule catalog is implemented
- no audit workflow beyond retained artifacts is implemented

### Technical Scope Gaps

- metrics persistence currently supports Delta only
- the framework assumes Spark and Great Expectations as core technology choices
- table validation requires an active Spark session
- supported datasets are defined by available mapping entries and suite artifacts

## Key Decisions a Business Analyst Will Likely Need to Clarify

To turn the current solution into a formal specification, these topics will likely require business decisions or stakeholder alignment.

- which user personas are officially in scope
- which datasets must be supported in the first release or phase
- whether validation is required, optional, blocking, or advisory for each use case
- what constitutes failure severity and escalation level
- when sampling is acceptable and when full validation is mandatory
- whether retained metrics are mandatory, optional, or environment-specific
- whether rule management remains code-based or requires a managed user workflow
- what audit, reporting, and evidence retention period is required
- what downstream actions should occur after validation success or failure
- whether a business-facing reporting or dashboard requirement exists

## Likely Requirements Themes for Future Specification Work

Without writing the specification itself, the implementation strongly suggests the future requirements will probably group into themes like these:

- validation execution and user interaction requirements
- dataset registration and suite mapping requirements
- rule management and change control requirements
- result presentation and evidence retention requirements
- metrics history and trend analysis requirements
- exception handling and operational response requirements
- environment and platform dependency requirements
- security, access, and governance requirements
- non-functional requirements such as performance, traceability, and maintainability

## Recommended Framing for the BA Handoff

The most accurate business framing of the current solution is:

The repository contains a configuration-driven data quality validation capability for Spark data, implemented as a thin framework over Great Expectations. It standardizes how datasets are validated against predefined rule sets, simplifies the returned output for consumers, and optionally retains both run evidence and expectation-level metrics for analysis. The current implementation is strong enough to describe a solution baseline, but it is not yet a complete end-user product specification.

## Summary Statement

In its current form, the solution should be understood as a reusable validation service layer for Spark-based datasets. Its core value lies in consistent execution, centralized dataset-to-suite mapping, simplified run results, optional historical metrics retention, and practical notebook or pipeline consumption. A BA can use this as the factual implementation baseline from which to derive formal business requirements, user stories, operating rules, and non-functional expectations.