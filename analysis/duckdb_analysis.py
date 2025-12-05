"""Entry point for running the DuckDB/Postgres analysis pipeline."""
from __future__ import annotations
import os
import time
from pathlib import Path
from typing import List, Tuple

import duckdb
from minio.error import S3Error
from connectors.db import setup_duckdb_with_postgres
from connectors.discovery import discover_h5_files
from connectors.minio import build_minio_client, log_minio_connection, resolve_minio_settings
from processing.h5_processing import HDF5OpenError, h5_to_dataframe
from utils.common import validate_h5_file
from pipeline_core import PipelineConfig, build_parser, configure_logging, ensure_directories, validate_source
from pipeline_core.data_loader import sanitize_parts


def process_file(
    con: duckdb.DuckDBPyConnection,
    file_label: str,
    file_path: Path,
    config: PipelineConfig,
    *,
    logger,
) -> None:
    """Process a single HDF5 file and persist results into Postgres via DuckDB."""

    file_start = time.perf_counter()

    if not validate_h5_file(file_path, logger=logger):
        return

    try:
        dataframe = h5_to_dataframe(file_path, config=config, logger=logger, display_name=file_label)
    except HDF5OpenError as exc:
        logger.error("Skipping %s due to unreadable HDF5: %s", file_label, exc)
        return
    except Exception as exc:  # noqa: BLE001
        logger.exception("Skipping %s due to unexpected processing error: %s", file_label, exc)
        return

    if dataframe is None or dataframe.is_empty():
        logger.error("Skipping %s: no usable data produced", file_label)
        return

    logger.info("⏱️  h5_to_dataframe took %.3f seconds", time.perf_counter() - file_start)

    table_suffix = sanitize_parts([file_label])
    table_name = f"job_{table_suffix}"
    df_name = f"dataframe_{table_suffix}"
    logger.info("Using PostgreSQL table name: %s", table_name)

    try:
        logger.info("Registering dataframe in DuckDB...")
        register_start = time.perf_counter()
        try:
            con.register(df_name, dataframe)
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "Registering via Arrow failed for %s, falling back to pandas: %s",
                file_label,
                exc,
            )
            con.register(df_name, dataframe.to_pandas())

        logger.info(
            "⏱️  DataFrame registration took %.3f seconds",
            time.perf_counter() - register_start,
        )

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
        return

    logger.info("⏱️  Total file processing took %.3f seconds", time.perf_counter() - file_start)


def run_pipeline() -> None:
    """Main entrypoint to process HDF5 files into Postgres via DuckDB."""

    pipeline_start = time.perf_counter()
    minio_settings = resolve_minio_settings()
    base_dir = Path(__file__).resolve().parent
    config = PipelineConfig.from_args(build_parser().parse_args([]), base_dir=base_dir)
    logger = configure_logging(config.log_file)

    log_minio_connection(minio_settings, logger=logger)
    minio_client = build_minio_client(minio_settings)

    try:
        for b in minio_client.list_buckets():
            logger.info("Bucket detected: %s", b.name)
    except S3Error as exc:
        raise RuntimeError("Unable to list buckets - verify access key/secret pair.") from exc

    logger.info("Step 1/4: validating configuration and preparing directories.")
    step_start = time.perf_counter()
    validate_source(config)
    ensure_directories(config)
    logger.info("⏱️  Configuration validation took %.3f seconds", time.perf_counter() - step_start)

    h5_files: List[Tuple[str, Path]] = discover_h5_files(
        config, minio_client=minio_client, minio_settings=minio_settings, logger=logger
    )

    logger.info("Step 3/4: Processing HDF5 files...")
    step3_start = time.perf_counter()
    con = setup_duckdb_with_postgres(password=os.getenv("POSTGRES_PASSWORD", ""), logger=logger)

    for idx, (file_label, file_path) in enumerate(h5_files, 1):
        logger.info("=" * 60)
        logger.info("Processing file %d/%d: %s", idx, len(h5_files), file_label)
        process_file(con, file_label, file_path, config, logger=logger)

    logger.info("⏱️  Step 3 (all files) took %.3f seconds", time.perf_counter() - step3_start)
    logger.info("=" * 60)
    logger.info("🎉 Pipeline completed successfully!")
    logger.info("⏱️  Total pipeline execution took %.3f seconds", time.perf_counter() - pipeline_start)


if __name__ == "__main__":
    run_pipeline()
