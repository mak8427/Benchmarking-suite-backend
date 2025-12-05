"""Helpers for locating HDF5 files."""

from __future__ import annotations

from pathlib import Path
from typing import List

from .config import PipelineConfig


def collect_h5_files(config: PipelineConfig, keep_batch_files: bool) -> List[Path]:
    """Return a sorted list of HDF5 files under the configured source path."""

    file_paths = sorted(config.source_dir.rglob("*.h5"))
    if keep_batch_files:
        return file_paths
    return [path for path in file_paths if "batch" not in path.stem]
