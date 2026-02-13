"""Helpers for locating HDF5 files."""

from __future__ import annotations

from pathlib import Path
from typing import List

from .config import PipelineConfig


def collect_h5_files(config: PipelineConfig, keep_batch_files: bool) -> List[Path]:
    """Return sorted HDF5 input files from the configured source directory.

    Args:
        config (PipelineConfig): Runtime pipeline settings with source dir.
        keep_batch_files (bool): Keep filenames containing ``batch`` when true.

    Returns:
        List[Path]: Sorted list of discovered HDF5 files.

    Examples:
        >>> from tempfile import TemporaryDirectory
        >>> from analysis_module.pipeline_core.config import PipelineConfig, PriceSettings
        >>> with TemporaryDirectory() as tmp:
        ...     base = Path(tmp)
        ...     src = base / "src"
        ...     src.mkdir()
        ...     _ = (src / "a.h5").write_bytes(b"1")
        ...     _ = (src / "batch_1.h5").write_bytes(b"1")
        ...     cfg = PipelineConfig(base, src, base / "out", base / "stats", base / "summary", base / "price", base / "run.log", False, PriceSettings(4169, "DE-LU", "quarterhour"), True)
        ...     [path.name for path in collect_h5_files(cfg, keep_batch_files=False)]
        ['a.h5']
    """

    file_paths = sorted(config.source_dir.rglob("*.h5"))
    if keep_batch_files:
        return file_paths
    return [path for path in file_paths if "batch" not in path.stem]
