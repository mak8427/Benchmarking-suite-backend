"""
Logging helpers for the analysis pipeline.
"""

from __future__ import annotations

import logging
from pathlib import Path


LOGGER_NAME = "analysis"


def configure_logging(log_path: Path) -> logging.Logger:
    """Configure and return the shared analysis logger.

    Args:
        log_path (Path): Destination log file path.

    Returns:
        logging.Logger: Configured logger with file and stream handlers.

    Examples:
        >>> from tempfile import TemporaryDirectory
        >>> with TemporaryDirectory() as tmp:
        ...     logger = configure_logging(Path(tmp) / "analysis.log")
        ...     logger.name
        'analysis'
    """
    logger = logging.getLogger(LOGGER_NAME)
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    log_path.parent.mkdir(parents=True, exist_ok=True)

    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")

    file_handler = logging.FileHandler(log_path, mode="w", encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    return logger
