from __future__ import annotations

from pathlib import Path

from gx_framework.logger import get_logger


def test_get_logger_creates_log_file(tmp_path: Path) -> None:
    logger = get_logger(tmp_path, log_level="INFO")
    logger.info("test_event", extra={"dataset_name": "customers"})

    log_files = list(tmp_path.glob("gx_validation_*.log"))

    assert len(log_files) == 1
    assert log_files[0].read_text(encoding="utf-8")


def test_get_logger_does_not_duplicate_handlers(tmp_path: Path) -> None:
    logger = get_logger(tmp_path, log_level="INFO")
    initial_handler_count = len(logger.handlers)

    logger = get_logger(tmp_path, log_level="INFO")

    assert len(logger.handlers) == initial_handler_count