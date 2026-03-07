---
name: python
description: Python coding standards, best practices, type hints, and testing patterns with uv for package management. Use when writing or reviewing Python code, implementing tests, setting up Python projects, managing dependencies with uv, working with virtual environments, adding type hints, writing pytest tests, or discussing Python language features and best practices.
---
# Python

- Use the latest Python language features appropriate for the project's minimum supported version.
- Always work within a virtual environment (never install packages globally).
- Use type hints for all function signatures and complex variables.
- Write tests using pytest with clear, descriptive test names.
- Follow PEP 8 conventions and use Google-style docstrings.

## Quick Start

For a new Python project:

```bash
# 1. Create virtual environment
uv venv

# 2. Activate it (optional, uv commands work without activation)
source .venv/bin/activate  # macOS/Linux

# 3. Add dependencies
uv add <package-name>

# 4. Run your script
uv run script.py
```

## Package Management

- Use `uv` for dependency management and package execution with virtual environments.
- **Always ensure a virtual environment exists** before running Python code.

### Virtual Environment Workflow

**Before any Python work, ensure a virtual environment exists:**

```bash
# Check if .venv exists
if [ ! -d ".venv" ]; then
    echo "Creating virtual environment..."
    uv venv
fi
```

**Complete workflow example:**

```bash
# 1. Create virtual environment (one-time setup)
uv venv
# Output: Using CPython 3.12.0
#         Creating virtual environment at: .venv

# 2. uv automatically detects and uses .venv for all operations
# No activation needed for uv commands!

# 3. Add a dependency
uv add requests
# Output: Resolved 5 packages in 142ms
#         Installed 5 packages in 23ms

# 4. Run your script (automatically uses .venv)
uv run script.py

# 5. Install dev dependencies
uv add --dev pytest ruff mypy

# 6. Sync environment with lock file
uv sync
```

**Manual activation (optional, for running Python directly):**

```bash
# macOS/Linux (bash/zsh)
source .venv/bin/activate

# Windows (Command Prompt)
.venv\Scripts\activate

# Windows (PowerShell)
.venv\Scripts\Activate.ps1

# Fish shell
source .venv/bin/activate.fish

# Deactivate when done
deactivate
```

**Key commands:**
- `uv run <script>` - Run script in virtual environment
- `uv add <package>` - Add production dependency
- `uv add --dev <package>` - Add development dependency
- `uv pip install <package>` - Direct pip-compatible install
- `uv sync` - Sync environment with lock file
- `uv pip list` - List installed packages
- `uv pip freeze` - Export requirements

**Troubleshooting:**
- If uv doesn't detect `.venv`, ensure you're in the project root
- Use `uv venv --python 3.11` to specify Python version
- Run `uv venv --help` for advanced options

## Documentation

When users ask about Python standard library modules, use `WebFetch` to get the latest official documentation from `docs.python.org`.

Example:
- For `asyncio`: `https://docs.python.org/3/library/asyncio.html`
- For `typing`: `https://docs.python.org/3/library/typing.html`
- For `pathlib`: `https://docs.python.org/3/library/pathlib.html`

Pattern: `https://docs.python.org/3/library/<module>.html`

Use Google-style docstrings for all functions, classes, and modules. Follow this format:

```python
def function_name(arg1: Type, arg2: Type) -> ReturnType:
    """One-line summary of what the function does.

    More detailed description if needed, explaining the purpose,
    behavior, and any important notes.

    Args:
        arg1: Description of arg1, including type and meaning.
        arg2: Description of arg2, including type and meaning.

    Returns:
        Description of the return value, including type.

    Raises:
        ExceptionType: Description of when this exception is raised.
        AnotherException: Another exception description.

    Examples:
        >>> function_name('example', 42)
        'result'
    """
```

For classes:
```python
class MyClass:
    """Brief description of the class.

    Longer description if needed.

    Attributes:
        attr1: Description of attr1.
        attr2: Description of attr2.
    """
```

## Type Hints

- **Always** use type hints for function signatures, class attributes, and variables where the type is not immediately obvious.
- **Avoid** using `Any` type. Use specific types, `TypeVar`, or protocols instead.
- **Avoid** using `# type: ignore` comments except in rare cases, mainly in test code.
- Use `from __future__ import annotations` for forward references and cleaner type hints.
- Prefer `list[T]`, `dict[K, V]`, `set[T]`, `tuple[T, ...]` over `typing.List`, `typing.Dict`, etc. (Python 3.9+).

Example:
```python
from __future__ import annotations

def process_items(items: list[str], max_count: int | None = None) -> dict[str, int]:
    """Process items and return a count dictionary."""
    result: dict[str, int] = {}
    for item in items[:max_count]:
        result[item] = result.get(item, 0) + 1
    return result
```

## Tests

Write parametrized tests using `pytest`:

```python
import pytest

@pytest.mark.parametrize(
    ("input_value", "expected"),
    [
        ("hello", "HELLO"),
        ("world", "WORLD"),
        ("", ""),
    ],
)
def test_uppercase(input_value: str, expected: str) -> None:
    assert my_function(input_value) == expected
```

- Use descriptive parameter names in test cases.
- Type hint test functions with `-> None`.
- Group related tests in classes when appropriate.
- Use fixtures for shared setup and teardown.

## Code Style

- Follow PEP 8 conventions.
- Use f-strings for string formatting.
- Maximum line length: 88 characters (Black default).
- Use trailing commas in multi-line structures.
- Prefer explicit over implicit ("Explicit is better than implicit").

## Common Patterns

### Project Initialization

```bash
# Start a new Python project
mkdir my-project && cd my-project
uv venv
uv add requests pytest ruff mypy
```

### Dependency Management

```bash
# Add packages
uv add pandas numpy  # Production dependencies
uv add --dev black pytest-cov  # Development dependencies

# Update packages
uv sync --upgrade

# Remove a package
uv remove package-name
```

### Type-Checked Function

```python
from __future__ import annotations

from pathlib import Path

def read_config(path: Path) -> dict[str, str | int]:
    """Read configuration from a file.

    Args:
        path: Path to the configuration file.

    Returns:
        Dictionary containing configuration key-value pairs.

    Raises:
        FileNotFoundError: If the configuration file doesn't exist.
        ValueError: If the file contains invalid configuration.
    """
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
   
    # Implementation here
    return {}
```

### Pytest Test Structure

```python
import pytest
from pathlib import Path
from mymodule import read_config

@pytest.fixture
def config_file(tmp_path: Path) -> Path:
    """Create a temporary config file for testing."""
    config = tmp_path / "config.ini"
    config.write_text("key=value")
    return config

@pytest.mark.parametrize(
    ("input_value", "expected"),
    [
        ("hello", "HELLO"),
        ("world", "WORLD"),
        ("", ""),
        ("Hello World!", "HELLO WORLD!"),
    ],
    ids=["simple", "another", "empty", "with-punctuation"],
)
def test_uppercase(input_value: str, expected: str) -> None:
    """Test uppercase conversion with various inputs."""
    assert my_function(input_value) == expected

def test_read_config_success(config_file: Path) -> None:
    """Test successful configuration reading."""
    result = read_config(config_file)
    assert isinstance(result, dict)

def test_read_config_missing_file() -> None:
    """Test error handling for missing configuration file."""
    with pytest.raises(FileNotFoundError, match="Config file not found"):
        read_config(Path("/nonexistent/config.ini"))
```

### Context Manager Pattern

```python
from typing import Generator
from contextlib import contextmanager

@contextmanager
def database_connection(url: str) -> Generator[Connection, None, None]:
    """Manage database connection lifecycle.
   
    Args:
        url: Database connection URL.
       
    Yields:
        Active database connection.
       
    Examples:
        >>> with database_connection("sqlite:///db.sqlite") as conn:
        ...     conn.execute("SELECT * FROM users")
    """
    conn = create_connection(url)
    try:
        yield conn
    finally:
        conn.close()
```