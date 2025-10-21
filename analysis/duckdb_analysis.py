
from __future__ import annotations
from __future__ import annotations

import h5py
from pathlib import Path

from analysis_pipeline import (
    PipelineConfig,
    build_parser,
    collect_h5_files,
    configure_logging,
    ensure_directories,
    validate_source,
)
from analysis_pipeline.data_loader import iter_datasets, dataset_to_polars
import polars as pl


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
                    combined = _combine_frames(frames)

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
                    print(combined)





if __name__ ==  "__main__":
    #1) Load the data
    base_dir = Path(__file__).resolve().parent
    print(f"Base directory: {base_dir}")
    config = PipelineConfig.from_args(build_parser().parse_args([]), base_dir=base_dir)
    print(f"Config: {config}")
    logger = configure_logging(config.log_file)



    logger.info("Step 1/4: validating configuration and preparing directories.")
    validate_source(config)
    ensure_directories(config)


    logger.info("Step 2/4: discovering input HDF5 files under %s.", config.source_dir)
    h5_files = collect_h5_files(config)
    print(f"h5_files: {h5_files}")
    if not h5_files:
        logger.warning("No .h5 files found in %s. Nothing to process.", config.source_dir)
        raise "no files"

    logger.info("Detected %d file(s) for analysis.", len(h5_files))


    for file_path in h5_files:
        dataframe = h5_to_dataframe(file_path, config, logger=logger)
        print(dataframe)
