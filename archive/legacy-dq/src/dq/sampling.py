"""Sampling utilities for PySpark DataFrames."""

from __future__ import annotations

import importlib
import math
from statistics import NormalDist
from typing import Any

DEFAULT_SAMPLING_CONFIG: dict[str, Any] = {
    "mode": "auto",  # auto | full | statistical | stratified
    "confidence": 0.95,
    "margin_error": 0.01,
    "max_sample_rows": 100000,
    "stratify_by": None,
    "seed": 42,
}


def calculate_sample_size(
    population_size: int,
    confidence: float = 0.95,
    margin_error: float = 0.01,
) -> int:
    """Calculate sample size using Cochran's formula with finite population correction."""
    if population_size <= 0:
        return 0
    if margin_error <= 0:
        raise ValueError("margin_error must be > 0")
    if not 0 < confidence < 1:
        raise ValueError("confidence must be between 0 and 1")

    z_score = NormalDist().inv_cdf((1.0 + confidence) / 2.0)
    p = 0.5
    n0 = (z_score**2 * p * (1.0 - p)) / (margin_error**2)
    n_adj = n0 / (1.0 + ((n0 - 1.0) / population_size))
    return min(math.ceil(n_adj), population_size)


def resolve_sampling_mode(mode: str, stratify_by: str | None) -> str:
    """Resolve final sampling mode, applying auto behavior."""
    mode_normalized = (mode or "auto").strip().lower()
    valid_modes = {"auto", "full", "statistical", "stratified"}
    if mode_normalized not in valid_modes:
        raise ValueError(f"Unsupported sampling mode: {mode}")

    if mode_normalized == "auto":
        return "stratified" if stratify_by else "statistical"
    return mode_normalized


def apply_sampling(
    df: Any,
    sampling_config: dict[str, Any] | None = None,
) -> tuple[Any, dict[str, Any]]:
    """Apply sampling strategy and return sampled DataFrame plus audit metadata."""
    config = dict(DEFAULT_SAMPLING_CONFIG)
    if sampling_config:
        config.update(sampling_config)

    mode = resolve_sampling_mode(config.get("mode", "auto"), config.get("stratify_by"))
    confidence = float(config.get("confidence", 0.95))
    margin_error = float(config.get("margin_error", 0.01))
    max_sample_rows = int(config.get("max_sample_rows", 100000))
    stratify_by = config.get("stratify_by")
    seed = int(config.get("seed", 42))

    original_row_count = int(df.count())
    base_meta = {
        "sampling_strategy": mode,
        "original_row_count": original_row_count,
        "sample_row_count": original_row_count,
        "confidence": confidence,
        "margin_error": margin_error,
        "stratify_by": stratify_by,
    }

    if original_row_count == 0 or mode == "full":
        base_meta["sampling_strategy"] = "full"
        return df, base_meta

    computed_n = calculate_sample_size(
        population_size=original_row_count,
        confidence=confidence,
        margin_error=margin_error,
    )
    target_n = min(computed_n, max_sample_rows)

    if target_n >= original_row_count:
        base_meta["sampling_strategy"] = "full"
        return df, base_meta

    if mode == "statistical":
        spark_functions = importlib.import_module("pyspark.sql.functions")
        rand = getattr(spark_functions, "rand")

        sampled_df = df.orderBy(rand(seed)).limit(target_n)
        sampled_count = int(sampled_df.count())
        return sampled_df, {
            **base_meta,
            "sampling_strategy": "statistical",
            "sample_row_count": sampled_count,
        }

    if mode == "stratified":
        spark_functions = importlib.import_module("pyspark.sql.functions")
        col = getattr(spark_functions, "col")
        rand = getattr(spark_functions, "rand")

        if not stratify_by:
            sampled_df = df.orderBy(rand(seed)).limit(target_n)
            sampled_count = int(sampled_df.count())
            return sampled_df, {
                **base_meta,
                "sampling_strategy": "statistical",
                "sample_row_count": sampled_count,
                "stratify_by": None,
            }

        fraction = min(1.0, float(target_n) / float(original_row_count))
        values = [r[0] for r in df.select(col(stratify_by)).distinct().collect()]
        fractions = {v: fraction for v in values}
        sampled_df = df.sampleBy(col=stratify_by, fractions=fractions, seed=seed)
        sampled_count = int(sampled_df.count())

        if sampled_count >= original_row_count:
            base_meta["sampling_strategy"] = "full"
            return df, base_meta

        return sampled_df, {
            **base_meta,
            "sampling_strategy": "stratified",
            "sample_row_count": sampled_count,
        }

    raise ValueError(f"Unsupported sampling mode after resolution: {mode}")
