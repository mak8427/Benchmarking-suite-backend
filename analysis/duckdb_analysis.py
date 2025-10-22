
from __future__ import annotations
import pandera as pa
import h5py
from pathlib import Path
import duckdb
import time
from functools import wraps
from typing import List, Tuple

from analysis_pipeline import (
    PipelineConfig,
    build_parser,
    collect_h5_files,
    configure_logging,
    ensure_directories,
    validate_source,
)
from analysis_pipeline.data_loader import dataset_prefix, dataset_to_polars, iter_datasets, sanitize_parts
from analysis_pipeline.pipeline import combine_frames
from analysis_pipeline.energy import (
    add_task_derivatives,
    build_summary_dataframe,
    compute_energy_profile,
)
import polars as pl


def timing_decorator(func):
    """Decorator to measure and log function execution time."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        # Try to get logger from kwargs
        logger = kwargs.get('logger')
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        elapsed = end_time - start_time

        if logger:
            logger.info(f"⏱️  {func.__name__} took {elapsed:.3f} seconds")
        else:
            print(f"⏱️  {func.__name__} took {elapsed:.3f} seconds")

        return result
    return wrapper


@timing_decorator
def cast_all_columns(df: pl.DataFrame, *, logger=None) -> pl.DataFrame:
    """
    Cast all columns in the DataFrame to appropriate types based on their names.
    Handles invalid values by clipping or setting to null.

    :param df: Input DataFrame
    :return: DataFrame with optimized dtypes
    """
    cast_exprs = []

    for column in df.columns:
        # Core time columns - unsigned integers with overflow protection
        if column == "ElapsedTime":
            MAX_ELAPSED = 2**63 - 1  # More conservative limit
            cast_exprs.append(
                pl.when(pl.col(column).is_finite() & pl.col(column).is_between(0, MAX_ELAPSED))
                .then(pl.col(column))
                .otherwise(None)
                .cast(pl.UInt64)  # Cast AFTER filtering
                .alias(column)
            )
        elif column == "ElapsedTime_Diff":
            MAX_ELAPSED_DIFF = 365 * 24 * 3600 * 1000  # 1 year in ms
            cast_exprs.append(
                pl.when(pl.col(column).is_finite() & pl.col(column).is_between(0, MAX_ELAPSED_DIFF))
                .then(pl.col(column))
                .otherwise(None)
                .cast(pl.UInt64)
                .alias(column)
            )

        # Epoch time columns - signed integers with range validation
        elif column == "EpochTime" or column.endswith("__EpochTime"):
            MIN_EPOCH, MAX_EPOCH = 0, 4102444800
            cast_exprs.append(
                pl.when(pl.col(column).is_finite() & pl.col(column).is_between(MIN_EPOCH, MAX_EPOCH))
                .then(pl.col(column))
                .otherwise(None)
                .cast(pl.Int64)
                .alias(column)
            )

        # CPU frequency - unsigned integer (MHz) with realistic bounds
        elif column.endswith("__CPUFrequency"):
            cast_exprs.append(
                pl.when(pl.col(column).is_finite() & pl.col(column).is_between(0, 10000))
                .then(pl.col(column))
                .otherwise(None)
                .cast(pl.UInt16)
                .alias(column)
            )

        # Memory columns - unsigned integers with overflow protection
        elif column.endswith("__RSS") or column.endswith("__VMSize"):
            cast_exprs.append(
                pl.when(pl.col(column).is_finite() & pl.col(column).is_between(0, 2**63 - 1))
                .then(pl.col(column))
                .otherwise(None)
                .cast(pl.UInt64)
                .alias(column)
            )
        elif column.endswith("__GPUMemMB") or column.endswith("__RSS_MB"):
            cast_exprs.append(
                pl.when(pl.col(column).is_finite() & pl.col(column).is_between(0, 1e6))
                .then(pl.col(column))
                .otherwise(None)
                .cast(pl.Float32)
                .alias(column)
            )

        # Page counts - unsigned integer with realistic bounds
        elif column.endswith("__Pages"):
            cast_exprs.append(
                pl.when(pl.col(column).is_finite() & pl.col(column).is_between(0, 2**32 - 1))
                .then(pl.col(column))
                .otherwise(None)
                .cast(pl.UInt32)
                .alias(column)
            )

        # Power and energy - float32 with non-negative constraint
        elif column == "NodePower" or column.endswith("__NodePower"):
            cast_exprs.append(
                pl.when(pl.col(column).is_finite() & pl.col(column).is_between(0, 10000))
                .then(pl.col(column))
                .otherwise(None)
                .cast(pl.Float32)
                .alias(column)
            )
        elif column in ("Energy_Increment_J", "Energy_used_J"):
            cast_exprs.append(
                pl.when(pl.col(column).is_finite() & (pl.col(column) >= 0))
                .then(pl.col(column))
                .otherwise(None)
                .cast(pl.Float32)
                .alias(column)
            )

        # Utilization percentages - float32 (0-100 range)
        elif column.endswith("__CPUUtilization") or column.endswith("__GPUUtilization"):
            cast_exprs.append(
                pl.when(pl.col(column).is_finite() & pl.col(column).is_between(0, 100))
                .then(pl.col(column))
                .otherwise(None)
                .cast(pl.Float32)
                .alias(column)
            )
        elif column.endswith("__CPUUtilization_normalized"):
            cast_exprs.append(
                pl.when(pl.col(column).is_finite() & pl.col(column).is_between(0, 1))
                .then(pl.col(column))
                .otherwise(None)
                .cast(pl.Float32)
                .alias(column)
            )

        # Time measurements - float32 with non-negative constraint
        elif column.endswith("__CPUTime"):
            cast_exprs.append(
                pl.when(pl.col(column).is_finite() & (pl.col(column) >= 0))
                .then(pl.col(column))
                .otherwise(None)
                .cast(pl.Float32)
                .alias(column)
            )

        # I/O metrics - float32 with non-negative constraint
        elif column.endswith("__ReadMB") or column.endswith("__WriteMB"):
            cast_exprs.append(
                pl.when(pl.col(column).is_finite() & (pl.col(column) >= 0))
                .then(pl.col(column))
                .otherwise(None)
                .cast(pl.Float32)
                .alias(column)
            )

    if cast_exprs:
        df = df.with_columns(cast_exprs)

    return df




@timing_decorator
def h5_to_dataframe(file_path: Path, config: PipelineConfig, *, logger) -> pl.dataframe.frame.DataFrame:
    """Process a single HDF5 file and returns a Polars Df

    Args:
        file_path: Path to the HDF5 file being processed.
        config: Pipeline configuration and output directories.
        logger: Logger used for status updates.
    """
    # Extract job identifier from filename
    job_id = file_path.stem.split("_")[0] if "_" in file_path.stem else file_path.stem
    logger.info("Processing %s", file_path.name)

    with h5py.File(file_path, "r") as h5_file:
        # Process each top-level group in the HDF5 file
        for group_name, group_node in h5_file.items():
            # Collect all datasets from this group (handles both flat datasets and nested structures)
            path_prefix = [group_name]
            datasets = (
                [(path_prefix, group_node)]
                if isinstance(group_node, h5py.Dataset)
                else list(iter_datasets(group_node, path_prefix))
            )

            # Convert each dataset to polars DataFrame with validation
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

                # Skip empty datasets
                if df.is_empty():
                    logger.warning(
                        "Data missing for %s in %s: dataset empty",
                        "/".join(dataset_path_parts),
                        file_path.name,
                    )
                    continue

                # Validate NodePower data (skip if all zeros)
                if "NodePower" in df.columns:
                    node_power = df["NodePower"].fill_null(0)
                    if node_power.sum() == 0:
                        logger.warning(
                            "Data missing for %s in %s: NodePower contains only zeros",
                            "/".join(dataset_path_parts),
                            file_path.name,
                        )
                        continue

                # Ensure ElapsedTime column exists and normalize it
                if "ElapsedTime" not in df.columns:
                    df = df.with_row_index("ElapsedTime")


                df = df.sort("ElapsedTime").with_columns(
                    pl.col("ElapsedTime").cast(pl.UInt64)
                )

                prefix = dataset_prefix(dataset_path_parts)
                frames.append((prefix, df))


            # Skip group if no valid datasets found
            if not frames:
                logger.warning(
                    "No usable datasets for %s in %s",
                    group_name,
                    file_path.name,
                )
                continue

            # Merge all frames into single time-aligned DataFrame
            logger.info("Combining %d frames...", len(frames))
            start_time = time.perf_counter()
            combined = combine_frames(frames)
            logger.info("⏱️  combine_frames took %.3f seconds", time.perf_counter() - start_time)

            # Identify and standardize epoch time column
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

            # Compute derived metrics and energy profile
            logger.info("Computing task derivatives...")
            start_time = time.perf_counter()
            combined = add_task_derivatives(combined)
            logger.info("⏱️  add_task_derivatives took %.3f seconds", time.perf_counter() - start_time)

            logger.info("Computing energy profile...")
            start_time = time.perf_counter()
            combined, metrics = compute_energy_profile(
                combined, job_id, group_name, logger=logger
            )
            logger.info("⏱️  compute_energy_profile took %.3f seconds", time.perf_counter() - start_time)

            # Integrate pricing data if enabled
            price_df = None
            active_epoch_column = (
                "EpochTime" if "EpochTime" in combined.columns else epoch_column
            )
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

            # Add cast for every column
            logger.info("Casting columns to optimized types...")
            combined = cast_all_columns(combined, logger=logger)

            # Write final combined data and statistics
            return combined



if __name__ ==  "__main__":
    pipeline_start = time.perf_counter()

    #1) Load the data
    base_dir = Path(__file__).resolve().parent
    print(f"Base directory: {base_dir}")
    config = PipelineConfig.from_args(build_parser().parse_args([]), base_dir=base_dir)
    print(f"Config: {config}")
    logger = configure_logging(config.log_file)



    logger.info("Step 1/4: validating configuration and preparing directories.")
    step_start = time.perf_counter()
    validate_source(config)
    ensure_directories(config)
    logger.info("⏱️  Configuration validation took %.3f seconds", time.perf_counter() - step_start)


    logger.info("Step 2/4: discovering input HDF5 files under %s.", config.source_dir)
    step_start = time.perf_counter()
    h5_files = collect_h5_files(config)
    logger.info("⏱️  File discovery took %.3f seconds", time.perf_counter() - step_start)

    print(f"h5_files: {h5_files}")
    if not h5_files:
        logger.warning("No .h5 files found in %s. Nothing to process.", config.source_dir)
        raise "no files"

    logger.info("Detected %d file(s) for analysis.", len(h5_files))


    logger.info("Step 3/4: Processing HDF5 files...")
    for idx, file_path in enumerate(h5_files, 1):
        logger.info("=" * 60)
        logger.info("Processing file %d/%d: %s", idx, len(h5_files), file_path.name)
        file_start = time.perf_counter()

        dataframe = h5_to_dataframe(file_path, config, logger=logger)

        logger.info("⏱️  Total file processing took %.3f seconds", time.perf_counter() - file_start)

        logger.info("Executing DuckDB query...")
        query_start = time.perf_counter()
        duckdb.sql("SELECT * FROM dataframe").show()
        logger.info("⏱️  DuckDB query took %.3f seconds", time.perf_counter() - query_start)

    logger.info("=" * 60)
    logger.info("🎉 Pipeline completed successfully!")
    logger.info("⏱️  Total pipeline execution took %.3f seconds", time.perf_counter() - pipeline_start)
