"""Discovery helper that merges local and remote HDF5 sources."""

from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Any, List, Tuple

from analysis_module.connectors.minio import download_minio_object, list_minio_objects
from analysis_module.pipeline_core import PipelineConfig, collect_h5_files


def discover_h5_files(
    config: PipelineConfig,
    *,
    minio_client: Any,
    minio_settings: dict[str, str | bool],
    logger,
) -> List[Tuple[str, Path]]:
    """Collect local and optional remote HDF5 files.

    Args:
        config (PipelineConfig): Pipeline configuration.
        minio_client: S3-compatible client for remote discovery.
        minio_settings (dict[str, str | bool]): MinIO connection settings.
        logger: Logger for diagnostics.

    Returns:
        List[Tuple[str, Path]]: File label/path pairs.

    Raises:
        RuntimeError: Raised when no files are discovered.

    Examples:
        >>> discover_h5_files.__name__
        'discover_h5_files'
    """
    logger.info("Step 2/4: discovering input HDF5 files under %s.", config.source_dir)
    step_start = time.perf_counter()
    local_files = collect_h5_files(config, keep_batch_files=False)
    remote_names = (
        list_minio_objects(
            minio_client,
            minio_settings["bucket"],
            minio_settings["prefix"],
            logger=logger,
        )
        if os.getenv("S3_SYNC") or os.getenv("MINIO_SYNC")
        else []
    )

    h5_files: List[Tuple[str, Path]] = []
    if local_files:
        h5_files.extend([(file_path.stem, file_path) for file_path in local_files])
    for object_name in remote_names:
        path = download_minio_object(
            minio_client,
            minio_settings["bucket"],
            object_name,
            logger=logger,
        )
        h5_files.append((Path(object_name).stem, path))
    logger.info("⏱️  File discovery took %.3f seconds", time.perf_counter() - step_start)

    if not h5_files:
        logger.warning("No .h5 files found. Nothing to process.")
        raise RuntimeError("no files")

    logger.info("Detected %d file(s) for analysis.", len(h5_files))
    return h5_files
