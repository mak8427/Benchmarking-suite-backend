from __future__ import annotations

import os
import time
from functools import wraps
from pathlib import Path

import h5py


def _truthy(value: str | None) -> bool:
    """Return True when *value* represents a truthy string."""

    if value is None:
        return False
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _mask_secret(value: str | None, visible: int = 4) -> str:
    """Return a masked representation of sensitive strings."""

    if not value:
        return "<missing>"
    if len(value) <= visible:
        return "*" * len(value)
    return f"{value[:visible]}***"


def validate_h5_file(file_path: Path, *, logger) -> bool:
    """Return True when the HDF5 file looks readable; otherwise log and return False."""

    if not file_path.exists():
        logger.error("Skipping %s: file does not exist", file_path)
        return False

    try:
        size = file_path.stat().st_size
    except OSError as exc:  # noqa: BLE001
        logger.error("Skipping %s: cannot stat file (%s)", file_path, exc)
        return False

    if size == 0:
        logger.error("Skipping %s: file is empty", file_path)
        return False

    try:
        if not h5py.is_hdf5(file_path):
            logger.error("Skipping %s: not a valid HDF5 file", file_path)
            return False
    except Exception as exc:  # noqa: BLE001
        logger.error("Skipping %s: HDF5 validation failed (%s)", file_path, exc)
        return False

    return True


def timing_decorator(func):
    """Decorator to measure and log function execution time."""

    @wraps(func)
    def wrapper(*args, **kwargs):
        logger = kwargs.get("logger")
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        elapsed = end_time - start_time

        if logger:
            logger.info("⏱️  %s took %.3f seconds", func.__name__, elapsed)
        else:
            print(f"⏱️  {func.__name__} took {elapsed:.3f} seconds")

        return result

    return wrapper
