"""Public API for the simplified Great Expectations framework."""

from .validator import validate_dataframe, validate_table

__all__ = ["validate_dataframe", "validate_table"]