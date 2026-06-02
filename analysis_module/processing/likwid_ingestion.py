"""LIKWID sibling discovery and ingestion helpers."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import polars as pl

from analysis_module.connectors.minio import download_minio_object, list_minio_objects
from analysis_module.connectors.normalized import (
    derive_job_metadata,
    infer_owner_metadata,
    write_likwid_samples,
)
from analysis_module.processing.likwid import match_elapsed_times, parse_likwid_file


def energy_elapsed_values(dataframe: pl.DataFrame) -> list[float]:
    """Return valid elapsed-time values from a processed HDF5 dataframe."""
    if "ElapsedTime" not in dataframe.columns:
        return []
    return [
        float(value)
        for value in dataframe["ElapsedTime"].to_list()
        if value is not None and 0 <= float(value) <= 3600
    ]


def ingest_related_likwid_objects(
    con,
    dataframe: pl.DataFrame,
    *,
    h5_file_label: str,
    minio_client: Any,
    minio_settings: dict[str, str | bool],
    logger,
) -> None:
    """Find and ingest LIKWID CSV/stderr siblings for a processed HDF5 object."""
    if not minio_client or not ("/" in h5_file_label):
        return
    metadata = infer_owner_metadata(h5_file_label)
    job_metadata = derive_job_metadata(metadata["original_filename"])
    job_id = job_metadata["job_id"]
    if not job_id:
        return

    prefix = h5_file_label.rsplit("/", 1)[0] + "/"
    try:
        candidates = list_minio_objects(
            minio_client,
            str(minio_settings["bucket"]),
            prefix,
            logger=logger,
            suffixes=(".csv", ".err"),
        )
    except RuntimeError as exc:
        logger.warning(
            "Skipping LIKWID sibling discovery for %s: %s", h5_file_label, exc
        )
        return

    related = [name for name in candidates if job_id in Path(name).name]
    if not related:
        logger.info("No LIKWID siblings found for %s", h5_file_label)
        return

    elapsed_values = energy_elapsed_values(dataframe)
    for object_name in related:
        path = download_minio_object(
            minio_client,
            str(minio_settings["bucket"]),
            object_name,
            logger=logger,
        )
        try:
            parsed = parse_likwid_file(path)
            rows = match_elapsed_times(parsed.rows, elapsed_values)
            write_likwid_samples(
                con,
                rows,
                h5_file_label=h5_file_label,
                source_object_key=object_name,
                source_kind=parsed.source_kind,
                logger=logger,
            )
        finally:
            try:
                path.unlink()
            except OSError:
                pass
