"""
End-to-end orchestration for processing HDF5 energy datasets.
"""

from __future__ import annotations

from pathlib import Path
from typing import List, Tuple

import h5py
import polars as pl

from .config import PipelineConfig, ensure_directories, validate_source
from .data_loader import dataset_prefix, dataset_to_polars, iter_datasets, sanitize_parts
from .energy import (
    add_task_derivatives,
    build_summary_dataframe,
    compute_energy_profile,
)
from .pricing import integrate_price_data


def _combine_frames(frames: List[Tuple[str, pl.DataFrame]]) -> pl.DataFrame:
    """Merge task/energy frames on elapsed time and interpolate numeric fields.

    Args:
        frames: List of (prefix, dataframe) pairs to merge.

    Returns:
        A unified DataFrame indexed by elapsed time.
    """
    timeline = pl.concat(
        [frame.select("ElapsedTime") for _, frame in frames],
        how="vertical",
    ).unique().sort("ElapsedTime")

    combined = timeline
    for prefix, frame in frames:
        rename_map = {
            column: f"{prefix}__{column}"
            for column in frame.columns
            if column != "ElapsedTime"
        }
        joined = frame.rename(rename_map)
        combined = combined.join(joined, on="ElapsedTime", how="left")

    combined = combined.sort("ElapsedTime")

    interpolation_columns = [
        column
        for column, dtype in combined.schema.items()
        if column != "ElapsedTime" and getattr(dtype, "is_numeric", lambda: False)()
    ]

    if interpolation_columns:
        combined = combined.with_columns(
            [
                pl.col(column)
                .cast(pl.Float64)
                .interpolate()
                .alias(column)
                for column in interpolation_columns
            ]
        )

    return combined


def collect_h5_files(config: PipelineConfig) -> List[Path]:
    """Return a sorted list of HDF5 files under the configured source path.

    Args:
        config: Pipeline configuration containing the source path.

    Returns:
        Sorted list of HDF5 file paths.
    """
    return sorted(config.source_dir.glob("*.h5"))


def process_h5_file(file_path: Path, config: PipelineConfig, *, logger) -> None:
    """Process a single HDF5 file: export data, stats, summaries, and pricing.

    Args:
        file_path: Path to the HDF5 file being processed.
        config: Pipeline configuration and output directories.
        logger: Logger used for status updates.
    """
    job_id = file_path.stem.split("_")[0] if "_" in file_path.stem else file_path.stem
    logger.info("Processing %s", file_path.name)

    with h5py.File(file_path, "r") as h5_file:
        for group_name, group_node in h5_file.items():
            path_prefix = [group_name]
            datasets = (
                [(path_prefix, group_node)]
                if isinstance(group_node, h5py.Dataset)
                else list(iter_datasets(group_node, path_prefix))
            )

            frames: List[Tuple[str, pl.DataFrame]] = []
            for dataset_path_parts, dataset in datasets:
                try:
                    df = dataset_to_polars(dataset)
                except Exception as exc:  # noqa: BLE001
                    logger.exception(
                        "Skipping dataset %s in %s due to error: %s",
                        "/".join(dataset_path_parts),
                        file_path.name,
                        exc,
                    )
                    continue

                if df.is_empty():
                    logger.warning(
                        "Data missing for %s in %s: dataset empty",
                        "/".join(dataset_path_parts),
                        file_path.name,
                    )
                    continue

                if "NodePower" in df.columns:
                    node_power = df["NodePower"].fill_null(0)
                    if node_power.sum() == 0:
                        logger.warning(
                            "Data missing for %s in %s: NodePower contains only zeros",
                            "/".join(dataset_path_parts),
                            file_path.name,
                        )
                        continue

                if "ElapsedTime" not in df.columns:
                    df = df.with_row_count("ElapsedTime")

                df = df.sort("ElapsedTime").with_columns(
                    pl.col("ElapsedTime").cast(pl.UInt64)
                )
                prefix = dataset_prefix(dataset_path_parts)
                frames.append((prefix, df))

            if not frames:
                logger.warning(
                    "No usable datasets for %s in %s",
                    group_name,
                    file_path.name,
                )
                continue

            combined = _combine_frames(frames)

            epoch_candidates = [
                column for column in combined.columns if column.endswith("__EpochTime")
            ]
            epoch_column = None
            if "Energy__EpochTime" in combined.columns:
                epoch_column = "Energy__EpochTime"
            elif epoch_candidates:
                epoch_column = epoch_candidates[0]

            if epoch_column and "EpochTime" not in combined.columns:
                combined = combined.with_columns(
                    pl.col(epoch_column).cast(pl.Int64).alias("EpochTime")
                )

            combined = add_task_derivatives(combined)

            combined, metrics = compute_energy_profile(
                combined, job_id, group_name, logger=logger
            )

            output_name = sanitize_parts([file_path.stem, group_name, "combined"])
            data_output_path = config.output_dir / f"{output_name}.csv"
            stats_output_path = config.stats_dir / f"{output_name}_stats.csv"
            summary_output_path = config.summary_dir / f"{output_name}_summary.csv"
            price_output_path = (
                config.price_dir / f"{output_name}_price.csv" if config.fetch_price else None
            )

            if metrics:
                summary_df = build_summary_dataframe(job_id, group_name, metrics)
                summary_df.write_csv(summary_output_path)
                logger.info(
                    "Saved summary -> %s",
                    summary_output_path.relative_to(config.base_dir),
                )
                logger.info(metrics["appliance_description"])

            price_df = None
            active_epoch_column = (
                "EpochTime" if "EpochTime" in combined.columns else epoch_column
            )
            if config.fetch_price and active_epoch_column:
                combined, price_df = integrate_price_data(
                    combined,
                    active_epoch_column,
                    filter_id=config.price.filter_id,
                    region=config.price.region,
                    resolution=config.price.resolution,
                    logger=logger,
                )
                if price_df is not None and price_output_path is not None:
                    price_df.write_csv(price_output_path)
                    logger.info(
                        "Saved price data -> %s",
                        price_output_path.relative_to(config.base_dir),
                    )
                    if "Cumulative_cost_EUR" in combined.columns:
                        total_cost = combined.select(
                            pl.col("Cumulative_cost_EUR").max()
                        ).item()
                        logger.info(
                            "Estimated cumulative cost job=%s group=%s: %.2f EUR",
                            job_id,
                            group_name,
                            total_cost,
                        )
            elif config.fetch_price and not active_epoch_column:
                logger.warning(
                    "Skipping price integration for job=%s group=%s: no epoch column.",
                    job_id,
                    group_name,
                )

            combined.write_csv(data_output_path)

            stats_df = combined.describe()
            stats_df.write_csv(stats_output_path)

            logger.info(
                "Saved combined data -> %s",
                data_output_path.relative_to(config.base_dir),
            )
            logger.info(
                "Saved combined stats -> %s",
                stats_output_path.relative_to(config.base_dir),
            )
            logger.info(
                "Combined stats preview for %s in %s:\n%s",
                group_name,
                file_path.name,
                stats_df,
            )


def run_pipeline(config: PipelineConfig, *, logger) -> None:
    """Validate configuration and process all available HDF5 files.

    Args:
        config: Pipeline configuration specifying inputs and outputs.
        logger: Logger used for status updates.
    """
    validate_source(config)
    ensure_directories(config)

    h5_files = collect_h5_files(config)
    if not h5_files:
        logger.warning("No .h5 files found in %s", config.source_dir)
        return

    for file_path in h5_files:
        process_h5_file(file_path, config, logger=logger)
