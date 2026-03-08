"""Native sampling helpers for gx_framework validation flows."""

from __future__ import annotations

import importlib
import math
from statistics import NormalDist
from typing import Any

from .config import FrameworkConfig


DEFAULT_SAMPLING_CONFIG: dict[str, Any] = {
    "enabled": False,
    "mode": "auto",
    "confidence": 0.95,
    "margin_error": 0.01,
    "max_rows": 100000,
    "stratify_by": None,
    "seed": 42,
}


def build_sampling_config(
    framework_config: FrameworkConfig,
    sampling_config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Merge framework defaults with per-call sampling overrides."""
    config = {
        "enabled": framework_config.enable_sampling,
        "mode": framework_config.sampling_mode,
        "confidence": framework_config.sampling_confidence,
        "margin_error": framework_config.sampling_margin_error,
        "max_rows": framework_config.sampling_max_rows,
        "stratify_by": framework_config.sampling_stratify_by,
        "seed": framework_config.sampling_seed,
    }
    if sampling_config:
        config.update(sampling_config)
    return config


def calculate_sample_size(
    population_size: int,
    confidence: float = 0.95,
    margin_error: float = 0.01,
) -> int:
    """Calculate sample size using Cochran's formula with finite correction."""
    if population_size <= 0:
        return 0
    if margin_error <= 0:
        raise ValueError("margin_error must be > 0")
    if not 0 < confidence < 1:
        raise ValueError("confidence must be between 0 and 1")

    z_score = NormalDist().inv_cdf((1.0 + confidence) / 2.0)
    p_value = 0.5
    initial_sample_size = (z_score**2 * p_value * (1.0 - p_value)) / (margin_error**2)
    adjusted_sample_size = initial_sample_size / (
        1.0 + ((initial_sample_size - 1.0) / population_size)
    )
    return min(math.ceil(adjusted_sample_size), population_size)


def resolve_sampling_mode(mode: str, stratify_by: str | None) -> str:
    """Resolve the final sampling mode from config."""
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
    original_row_count: int | None = None,
) -> tuple[Any, dict[str, Any]]:
    """Apply sampling and return the DataFrame plus audit metadata."""
    config = dict(DEFAULT_SAMPLING_CONFIG)
    if sampling_config:
        config.update(sampling_config)

    enabled = bool(config.get("enabled", False))
    confidence = float(config.get("confidence", 0.95))
    margin_error = float(config.get("margin_error", 0.01))
    max_rows = int(config.get("max_rows", 100000))
    stratify_by = config.get("stratify_by")
    seed = int(config.get("seed", 42))

    row_count = original_row_count if original_row_count is not None else int(df.count())
    base_meta = {
        "sampling_strategy": "full",
        "original_row_count": row_count,
        "validated_row_count": row_count,
        "sampling_confidence": confidence,
        "sampling_margin_error": margin_error,
        "sampling_stratify_by": stratify_by,
    }

    if not enabled or row_count == 0:
        return df, base_meta

    mode = resolve_sampling_mode(str(config.get("mode", "auto")), stratify_by)
    if mode == "full":
        return df, base_meta

    target_rows = min(
        calculate_sample_size(
            population_size=row_count,
            confidence=confidence,
            margin_error=margin_error,
        ),
        max_rows,
    )
    if target_rows >= row_count:
        return df, base_meta

    if mode == "statistical":
        spark_functions = importlib.import_module("pyspark.sql.functions")
        rand = getattr(spark_functions, "rand")
        sampled_df = df.orderBy(rand(seed)).limit(target_rows)
        sampled_count = int(sampled_df.count())
        return sampled_df, {
            **base_meta,
            "sampling_strategy": "statistical",
            "validated_row_count": sampled_count,
        }

    spark_functions = importlib.import_module("pyspark.sql.functions")
    col = getattr(spark_functions, "col")

    if not stratify_by:
        return df, base_meta

    fraction = min(1.0, float(target_rows) / float(row_count))
    values = [row[0] for row in df.select(col(stratify_by)).distinct().collect()]
    fractions = {value: fraction for value in values}
    sampled_df = df.sampleBy(col=stratify_by, fractions=fractions, seed=seed)
    sampled_count = int(sampled_df.count())
    if sampled_count >= row_count:
        return df, base_meta

    return sampled_df, {
        **base_meta,
        "sampling_strategy": "stratified",
        "validated_row_count": sampled_count,
    }