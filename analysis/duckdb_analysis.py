
from __future__ import annotations

import os
import time
from functools import wraps
from pathlib import Path
from typing import List, Tuple

import duckdb
import h5py
import polars as pl


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


class HDF5OpenError(RuntimeError):
    """Raised when an HDF5 file cannot be opened (e.g., truncated)."""


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


def resolve_minio_settings() -> dict[str, str | bool]:
    """Collect MinIO/S3 connection details from environment variables."""

    access = os.getenv("MINIO_ACCESS_KEY") or os.getenv("AWS_ACCESS_KEY_ID")
    secret = os.getenv("MINIO_SECRET_KEY") or os.getenv("AWS_SECRET_ACCESS_KEY")
    endpoint = (
        os.getenv("MINIO_ADMIN_ENDPOINT")
        or os.getenv("MINIO_PUBLIC_ENDPOINT")
        or os.getenv("MINIO_ENDPOINT")
        or os.getenv("AWS_ENDPOINT_URL")
    )
    bucket = os.getenv("MINIO_BUCKET", "benchwrap")
    prefix = os.getenv("MINIO_OBJECT_PREFIX", "cane12345/")
    if prefix.startswith("/"):
        prefix = prefix[1:]
    if prefix and not prefix.endswith("/"):
        prefix = f"{prefix}/"
    secure = _truthy(os.getenv("MINIO_SECURE"))

    return {
        "access": access,
        "secret": secret,
        "endpoint": endpoint,
        "bucket": bucket,
        "prefix": prefix,
        "secure": secure,
    }


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
def h5_to_dataframe(
    file_path: Path, config: PipelineConfig, *, logger, display_name: str | None = None
) -> pl.dataframe.frame.DataFrame:
    """Process a single HDF5 file and returns a Polars Df

    Args:
        file_path: Path to the HDF5 file being processed.
        display_name: Friendly identifier for logging (defaults to filename).
        config: Pipeline configuration and output directories.
        logger: Logger used for status updates.
    """
    file_label = display_name or file_path.name
    job_id_source = file_label if display_name else file_path.stem
    job_id = job_id_source.split("_")[0] if "_" in job_id_source else job_id_source
    logger.info("Processing %s", file_label)

    try:
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
                            file_label,
                            exc,
                        )
                        continue

                # Skip empty datasets
                if df.is_empty():
                    logger.warning(
                        "Data missing for %s in %s: dataset empty",
                        "/".join(dataset_path_parts),
                        file_label,
                    )
                    continue

                # Validate NodePower data (skip if all zeros)
                if "NodePower" in df.columns:
                    node_power = df["NodePower"].fill_null(0)
                    if node_power.sum() == 0:
                        logger.warning(
                            "Data missing for %s in %s: NodePower contains only zeros",
                            "/".join(dataset_path_parts),
                            file_label,
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
                        file_label,
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
    except OSError as exc:
        try:
            size = file_path.stat().st_size
        except OSError:
            size = -1
        raise HDF5OpenError(
            f"Failed to open HDF5 file {file_path} (size={size} bytes). "
            "The file appears corrupted or truncated; re-download or remove it before retrying."
        ) from exc



if __name__ ==  "__main__":

    pipeline_start = time.perf_counter()

    from minio import Minio
    from minio.error import S3Error
    from tempfile import NamedTemporaryFile

    minio_settings = resolve_minio_settings()
    missing = [
        name
        for name in ("endpoint", "access", "secret")
        if not minio_settings.get(name)
    ]
    if missing:
        raise RuntimeError(
            "Missing MinIO configuration for: "
            + ", ".join(missing)
            + ". Ensure MINIO_ACCESS_KEY, MINIO_SECRET_KEY, and MINIO_ADMIN_ENDPOINT "
            "(or compatible variables) are defined."
        )

    print(
        "Connecting to MinIO endpoint",
        minio_settings["endpoint"],
        f"secure={minio_settings['secure']}",
    )
    print("Using access key:", _mask_secret(minio_settings["access"]))

    client = Minio(
        minio_settings["endpoint"],
        access_key=minio_settings["access"],
        secret_key=minio_settings["secret"],
        secure=bool(minio_settings["secure"]),
    )

    # List buckets
    try:
        for b in client.list_buckets():
            print("Bucket:", b.name)
    except S3Error as exc:
        raise RuntimeError(
            "Unable to list buckets - verify access key/secret pair."
        ) from exc

    # List objects in benchwrap prefix
    print(
        "Scanning bucket",
        minio_settings["bucket"],
        "prefix",
        minio_settings["prefix"],
    )
    try:
        for obj in client.list_objects(
            minio_settings["bucket"],
            prefix=minio_settings["prefix"],
            recursive=True,
        ):
            print("Object:", obj.object_name)
    except S3Error as exc:
        raise RuntimeError(
            f"Unable to list objects under {minio_settings['bucket']}/"
            f"{minio_settings['prefix']} - check bucket name and permissions."
        ) from exc


    def get_minio_object(bucket: str, object_name: str, client=client) -> Path:
        tmp = NamedTemporaryFile(delete=False, suffix=".h5")
        try:
            client.fget_object(bucket, object_name, tmp.name)
        except S3Error as exc:
            raise RuntimeError(
                f"Failed to download {bucket}/{object_name}: {exc.code}"
            ) from exc
        return Path(tmp.name)

    #1) Load the data
    base_dir = Path(__file__).resolve().parent
    password = os.getenv("POSTGRES_PASSWORD", "")
    print(f"Base directory: {base_dir}")
    config = PipelineConfig.from_args(build_parser().parse_args([]), base_dir=base_dir)
    print(f"Config: {config}")
    logger = configure_logging(config.log_file)

    minio_files = []
    try:
        for obj in client.list_objects(
            minio_settings["bucket"],
            prefix=minio_settings["prefix"],
            recursive=True,
        ):
            if obj.object_name.endswith(".h5"):
                minio_files.append(obj.object_name)
    except S3Error as exc:
        raise RuntimeError(
            "Unable to enumerate objects for processing"
        ) from exc

    if not minio_files:
        raise RuntimeError(
            f"No .h5 files found under {minio_settings['bucket']}"
            f"/{minio_settings['prefix']}"
        )




    logger.info("Step 1/4: validating configuration and preparing directories.")
    step_start = time.perf_counter()
    validate_source(config)
    ensure_directories(config)
    logger.info("⏱️  Configuration validation took %.3f seconds", time.perf_counter() - step_start)


    logger.info("Step 2/4: discovering input HDF5 files under %s.", config.source_dir)
    step_start = time.perf_counter()
    local_files = collect_h5_files(config)
    remote_files = [] if not os.getenv("MINIO_SYNC") else minio_files

    h5_files: List[Tuple[str, Path]] = []
    if local_files:
        h5_files.extend([(file_path.stem, file_path) for file_path in local_files])
    for object_name in remote_files:
        path = get_minio_object(minio_settings["bucket"], object_name)
        h5_files.append((Path(object_name).stem, path))
    logger.info("⏱️  File discovery took %.3f seconds", time.perf_counter() - step_start)

    print(f"h5_files: {[name for name, _ in h5_files]}")
    if not h5_files:
        logger.warning("No .h5 files found in %s. Nothing to process.", config.source_dir)
        raise "no files"

    logger.info("Detected %d file(s) for analysis.", len(h5_files))

    logger.info("Step 3/4: Processing HDF5 files...")
    step3_start = time.perf_counter()

    logger.info("Initializing DuckDB connection...")
    db_init_start = time.perf_counter()
    con = duckdb.connect()
    logger.info("⏱️  DuckDB connection created in %.3f seconds", time.perf_counter() - db_init_start)

    logger.info("Installing and loading PostgreSQL extension...")
    pg_setup_start = time.perf_counter()
    con.execute("INSTALL postgres;")
    con.execute("LOAD postgres;")
    logger.info("⏱️  PostgreSQL extension setup took %.3f seconds", time.perf_counter() - pg_setup_start)

    logger.info("Attaching to PostgreSQL database...")
    attach_start = time.perf_counter()
    conn_str = f"host=127.0.0.1 port=5432 dbname=postgres user=postgres password={password}"
    con.execute(f"ATTACH '{conn_str}' AS pg (TYPE postgres);")
    logger.info("⏱️  PostgreSQL attachment took %.3f seconds", time.perf_counter() - attach_start)

    for idx, (file_label, file_path) in enumerate(h5_files, 1):
        logger.info("=" * 60)
        logger.info("Processing file %d/%d: %s", idx, len(h5_files), file_label)
        file_start = time.perf_counter()

        if not validate_h5_file(file_path, logger=logger):
            continue

        try:
            dataframe = h5_to_dataframe(file_path, config=config, logger=logger, display_name=file_label)
        except HDF5OpenError as exc:
            logger.error("Skipping %s due to unreadable HDF5: %s", file_label, exc)
            continue
        except Exception as exc:  # noqa: BLE001
            logger.exception("Skipping %s due to unexpected processing error: %s", file_label, exc)
            continue

        if dataframe is None or dataframe.is_empty():
            logger.error("Skipping %s: no usable data produced", file_label)
            continue

        logger.info("⏱️  h5_to_dataframe took %.3f seconds", time.perf_counter() - file_start)

        table_suffix = sanitize_parts([file_label])
        table_name = f"job_{table_suffix}"
        df_name = f"dataframe_{table_suffix}"
        logger.info("Using PostgreSQL table name: %s", table_name)

        try:
            logger.info("Registering dataframe in DuckDB...")
            register_start = time.perf_counter()
            con.register(df_name, dataframe)
            logger.info("⏱️  DataFrame registration took %.3f seconds", time.perf_counter() - register_start)

            logger.info("Dropping existing PostgreSQL table if present...")
            drop_start = time.perf_counter()
            con.execute(f"DROP TABLE IF EXISTS pg.public.{table_name};")
            logger.info("⏱️  DROP TABLE took %.3f seconds", time.perf_counter() - drop_start)

            logger.info("Creating PostgreSQL table from dataframe...")
            create_start = time.perf_counter()
            con.execute(f"CREATE TABLE pg.public.{table_name} AS SELECT * FROM {df_name};")
            logger.info("⏱️  CREATE TABLE took %.3f seconds", time.perf_counter() - create_start)
        except Exception as exc:  # noqa: BLE001
            logger.exception("Failed to materialize table %s: %s", table_name, exc)
            continue

        logger.info("⏱️  Total file processing took %.3f seconds", time.perf_counter() - file_start)

    logger.info("⏱️  Step 3 (all files) took %.3f seconds", time.perf_counter() - step3_start)
    logger.info("=" * 60)
    logger.info("🎉 Pipeline completed successfully!")
    logger.info("⏱️  Total pipeline execution took %.3f seconds", time.perf_counter() - pipeline_start)
