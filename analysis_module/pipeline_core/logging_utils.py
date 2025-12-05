"""
Logging helpers for the analysis pipeline.
"""

from __future__ import annotations

import logging
from pathlib import Path


LOGGER_NAME = "analysis"


def configure_logging(log_path: Path) -> logging.Logger:
    """Initialise a dual file/stream logger writing to *log_path*."""
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
