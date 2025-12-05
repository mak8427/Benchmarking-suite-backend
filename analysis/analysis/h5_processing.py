from __future__ import annotations

import time
from pathlib import Path
from typing import List, Tuple

import h5py
import polars as pl

from analysis.utils.common import timing_decorator
from analysis.casting import cast_all_columns
from analysis_pipeline import PipelineConfig
from analysis_pipeline.data_loader import dataset_prefix, dataset_to_polars, iter_datasets
from analysis_pipeline.energy import add_task_derivatives, compute_energy_profile
from analysis_pipeline.combiner import combine_frames
from analysis_pipeline.pricing import integrate_price_data


class HDF5OpenError(RuntimeError):
    """Raised when an HDF5 file cannot be opened (e.g., truncated)."""


@timing_decorator
def h5_to_dataframe(
    file_path: Path, config: PipelineConfig, *, logger, display_name: str | None = None
) -> pl.dataframe.frame.DataFrame:
    """Process a single HDF5 file and returns a Polars DataFrame."""

    file_label = display_name or file_path.name
    job_id_source = file_label if display_name else file_path.stem
    job_id = job_id_source.split("_")[0] if "_" in job_id_source else job_id_source
    logger.info("Processing %s", file_label)

    file_warnings: List[dict[str, int]] = []

    try:
        with h5py.File(file_path, "r") as h5_file:
            # Process each top-level group in the HDF5 file
            for group_name, group_node in h5_file.items():
                path_prefix = [group_name]
                datasets = (
                    [(path_prefix, group_node)]
                    if isinstance(group_node, h5py.Dataset)
                    else list(iter_datasets(group_node, path_prefix))
                )

                frames: List[Tuple[str, pl.DataFrame]] = []
                group_counts = {"empty": 0, "zero_power": 0, "errors": 0, "total": 0}
                for dataset_path_parts, dataset in datasets:
                    group_counts["total"] += 1
                    try:
                        df = dataset_to_polars(dataset)
                    except Exception as exc:  # noqa: BLE001
                        group_counts["errors"] += 1
                        logger.exception(
                            "Skipping dataset %s in %s due to error: %s",
                            "/".join(dataset_path_parts),
                            file_label,
                            exc,
                        )
                        continue

                    if df.is_empty():
                        group_counts["empty"] += 1
                        logger.warning(
                            "Data missing for %s in %s: dataset empty",
                            "/".join(dataset_path_parts),
                            file_label,
                        )
                        continue

                    if "NodePower" in df.columns:
                        node_power = df["NodePower"].fill_null(0)
                        if node_power.sum() == 0:
                            group_counts["zero_power"] += 1
                            logger.warning(
                                "Data missing for %s in %s: NodePower contains only zeros",
                                "/".join(dataset_path_parts),
                                file_label,
                            )
                            continue

                    if "ElapsedTime" not in df.columns:
                        df = df.with_row_index("ElapsedTime")

                    df = df.sort("ElapsedTime").with_columns(pl.col("ElapsedTime").cast(pl.UInt64))

                    prefix = dataset_prefix(dataset_path_parts)
                    frames.append((prefix, df))

                if not frames:
                    file_warnings.append(group_counts)
                    logger.warning(
                        "No usable datasets for %s in %s (empty=%d, zero_power=%d, errors=%d, total=%d)",
                        group_name,
                        file_label,
                        group_counts["empty"],
                        group_counts["zero_power"],
                        group_counts["errors"],
                        group_counts["total"],
                    )
                    continue

                logger.info("Combining %d frames...", len(frames))
                start_time = time.perf_counter()
                combined = combine_frames(frames)
                logger.info("⏱️  combine_frames took %.3f seconds", time.perf_counter() - start_time)

                epoch_candidates = [column for column in combined.columns if column.endswith("__EpochTime")]
                epoch_column = None
                if "Energy__EpochTime" in combined.columns:
                    epoch_column = "Energy__EpochTime"
                elif epoch_candidates:
                    epoch_column = epoch_candidates[0]

                if epoch_column and "EpochTime" not in combined.columns:
                    combined = combined.with_columns(pl.col(epoch_column).cast(pl.Int64).alias("EpochTime"))

                logger.info("Computing task derivatives...")
                start_time = time.perf_counter()
                combined = add_task_derivatives(combined)
                logger.info("⏱️  add_task_derivatives took %.3f seconds", time.perf_counter() - start_time)

                logger.info("Computing energy profile...")
                start_time = time.perf_counter()
                combined, metrics = compute_energy_profile(combined, job_id, group_name, logger=logger)
                logger.info("⏱️  compute_energy_profile took %.3f seconds", time.perf_counter() - start_time)

                price_df = None
                active_epoch_column = "EpochTime" if "EpochTime" in combined.columns else epoch_column
                if config.fetch_price and active_epoch_column:
                    logger.info("Integrating price data...")
                    start_time = time.perf_counter()
                    combined, price_df = integrate_price_data(
                        combined,
                        active_epoch_column,
                        filter_id=config.price.filter_id,
                        region=config.price.region,
                        resolution=config.price.resolution,
                        logger=logger,
                    )
                    logger.info("⏱️  integrate_price_data took %.3f seconds", time.perf_counter() - start_time)
                elif config.fetch_price and not active_epoch_column:
                    logger.warning(
                        "Skipping price integration for job=%s group=%s: no epoch column.",
                        job_id,
                        group_name,
                    )

                logger.info("Casting columns to optimized types...")
                combined = cast_all_columns(combined, logger=logger)

                logger.info(
                    "Completed %s: rows=%d, cols=%d",
                    file_label,
                    combined.height,
                    combined.width,
                )
                return combined
    except OSError as exc:
        try:
            size = file_path.stat().st_size
        except OSError:
            size = -1
        raise HDF5OpenError(
            f"Failed to open HDF5 file {file_path} (size={size} bytes). "
            "The file appears corrupted or truncated; re-download or remove it before retrying."
        ) from exc
    if file_warnings:
        totals = {
            "empty": sum(entry["empty"] for entry in file_warnings),
            "zero_power": sum(entry["zero_power"] for entry in file_warnings),
            "errors": sum(entry["errors"] for entry in file_warnings),
            "total": sum(entry.get("total", 0) for entry in file_warnings),
        }
        logger.error(
            "No usable data produced for %s (empty datasets=%d, zero-power datasets=%d, loader errors=%d, total datasets=%d)",
            file_label,
            totals["empty"],
            totals["zero_power"],
            totals["errors"],
            totals["total"],
        )
    return None
