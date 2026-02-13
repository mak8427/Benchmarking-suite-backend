"""Shared helpers for analysis pipeline modules."""

from __future__ import annotations

import time
from functools import wraps
from pathlib import Path

import h5py


def _truthy(value: str | None) -> bool:
    """Return whether a string value should be treated as true.

    Args:
        value (str | None): Input string.

    Returns:
        bool: Parsed truthiness flag.

    Examples:
        >>> _truthy("yes")
        True
        >>> _truthy("0")
        False
    """
    if value is None:
        return False
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _mask_secret(value: str | None, visible: int = 4) -> str:
    """Return a masked representation of sensitive strings.

    Args:
        value (str | None): Secret value.
        visible (int): Number of visible prefix characters.

    Returns:
        str: Masked representation.

    Examples:
        >>> _mask_secret("abcdef", visible=2)
        'ab***'
        >>> _mask_secret(None)
        '<missing>'
    """
    if not value:
        return "<missing>"
    if len(value) <= visible:
        return "*" * len(value)
    return f"{value[:visible]}***"


def validate_h5_file(file_path: Path, *, logger) -> bool:
    """Return whether an HDF5 file is readable and non-empty.

    Args:
        file_path (Path): Candidate file path.
        logger: Logger instance for diagnostics.

    Returns:
        bool: ``True`` when file is valid for processing.

    Examples:
        >>> from pathlib import Path
        >>> validate_h5_file(Path('does-not-exist.h5'), logger=type('L', (), {'error': lambda *a, **k: None})())
        False
    """
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
    """Decorate a function to log elapsed execution time.

    Args:
        func: Wrapped callable.

    Returns:
        callable: Wrapped callable with timing logs.

    Examples:
        >>> class _L:
        ...     def info(self, *args, **kwargs):
        ...         return None
        >>> def f(x, *, logger=None):
        ...     return x + 1
        >>> wrapped = timing_decorator(f)
        >>> wrapped(1, logger=_L())
        2
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        """Invoke wrapped function and emit elapsed-time telemetry.

        Args:
            *args: Positional arguments for wrapped callable.
            **kwargs: Keyword arguments for wrapped callable.

        Returns:
            Any: Wrapped function return value.

        Examples:
            >>> wrapper.__name__
            'f'
        """
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
