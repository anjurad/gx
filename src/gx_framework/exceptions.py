"""Custom exception types for the simplified GX framework."""

from __future__ import annotations


class GXFrameworkError(Exception):
    """Base exception for framework-level errors."""


class ConfigurationError(GXFrameworkError):
    """Raised when framework configuration is invalid or unreadable."""


class GXContextError(GXFrameworkError):
    """Raised when the GX context is missing or cannot be initialized."""


class SuiteResolutionError(GXFrameworkError):
    """Raised when an expectation suite name cannot be resolved."""


class ValidationExecutionError(GXFrameworkError):
    """Raised when validation cannot be executed or fails critically."""