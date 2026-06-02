"""Entry point for running the DuckDB/Postgres analysis pipeline."""

from __future__ import annotations
import os
import time
from pathlib import Path
from typing import List, Tuple

import duckdb
from analysis_module.connectors.db import setup_duckdb_with_postgres
from analysis_module.connectors.discovery import discover_h5_files
from analysis_module.connectors.minio import (
    build_minio_client,
    log_minio_connection,
    resolve_minio_settings,
)
from analysis_module.connectors.normalized import (
    prepare_postgres_normalized_schema,
    ensure_normalized_schema,
    write_dashboard_tables,
)
from analysis_module.processing.h5_processing import HDF5OpenError, h5_to_dataframe
from analysis_module.processing.likwid_ingestion import ingest_related_likwid_objects
from analysis_module.utils.common import validate_h5_file
from analysis_module.pipeline_core import (
    PipelineConfig,
    build_parser,
    configure_logging,
    ensure_directories,
    validate_source,
)
from analysis_module.pipeline_core.data_loader import sanitize_parts


def process_file(
    con: duckdb.DuckDBPyConnection,
    file_label: str,
    file_path: Path,
    config: PipelineConfig,
    minio_client: object | None = None,
    minio_settings: dict[str, str | bool] | None = None,
    *,
    logger,
) -> None:
    """Process one HDF5 input and materialize it into PostgreSQL via DuckDB.

    Args:
        con (duckdb.DuckDBPyConnection): Active DuckDB connection.
        file_label (str): Human-readable file label used in logs/table naming.
        file_path (Path): Local path to the HDF5 file.
        config (PipelineConfig): Runtime pipeline settings.
        logger: Logger-like object providing ``info``/``warning``/``error``.

    Examples:
        >>> import analysis_module.duckdb_analysis as mod
        >>> class Logger:
        ...     def info(self, *args, **kwargs): pass
        ...     def warning(self, *args, **kwargs): pass
        ...     def error(self, *args, **kwargs): pass
        ...     def exception(self, *args, **kwargs): pass
        >>> class Conn:
        ...     def register(self, *args, **kwargs): raise AssertionError("unexpected register")
        ...     def execute(self, *args, **kwargs): raise AssertionError("unexpected execute")
        >>> old_validate = mod.validate_h5_file
        >>> mod.validate_h5_file = lambda *_args, **_kwargs: False
        >>> mod.process_file(Conn(), "job_1.h5", Path("missing.h5"), None, logger=Logger())
        >>> mod.validate_h5_file = old_validate
    """

    file_start = time.perf_counter()

    if not validate_h5_file(file_path, logger=logger):
        return

    try:
        dataframe = h5_to_dataframe(
            file_path, config=config, logger=logger, display_name=file_label
        )
    except HDF5OpenError as exc:
        logger.error("Skipping %s due to unreadable HDF5: %s", file_label, exc)
        return
    except Exception as exc:  # noqa: BLE001
        logger.exception(
            "Skipping %s due to unexpected processing error: %s", file_label, exc
        )
        return

    if dataframe is None or dataframe.is_empty():
        logger.error("Skipping %s: no usable data produced", file_label)
        return

    logger.info(
        "⏱️  h5_to_dataframe took %.3f seconds", time.perf_counter() - file_start
    )

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
        logger.info(
            "⏱️  DROP TABLE took %.3f seconds", time.perf_counter() - drop_start
        )

        logger.info("Creating PostgreSQL table from dataframe...")
        create_start = time.perf_counter()
        con.execute(f"CREATE TABLE pg.public.{table_name} AS SELECT * FROM {df_name};")
        logger.info(
            "⏱️  CREATE TABLE took %.3f seconds", time.perf_counter() - create_start
        )

        logger.info("Writing normalized dashboard tables...")
        dashboard_start = time.perf_counter()
        write_dashboard_tables(con, dataframe, file_label=file_label, logger=logger)
        if minio_client and minio_settings:
            ingest_related_likwid_objects(
                con,
                dataframe,
                h5_file_label=file_label,
                minio_client=minio_client,
                minio_settings=minio_settings,
                logger=logger,
            )
        logger.info(
            "⏱️  Dashboard table write took %.3f seconds",
            time.perf_counter() - dashboard_start,
        )
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to materialize table %s: %s", table_name, exc)
        return

    logger.info(
        "⏱️  Total file processing took %.3f seconds", time.perf_counter() - file_start
    )


def run_pipeline() -> None:
    """Run the full DuckDB/PostgreSQL pipeline workflow.

    The function resolves runtime config, discovers source files, and delegates
    per-file processing to :func:`process_file`.

    Examples:
        >>> parser = build_parser()
        >>> any(action.dest == "source" for action in parser._actions)
        True
    """

    pipeline_start = time.perf_counter()
    minio_settings = resolve_minio_settings()
    base_dir = Path(__file__).resolve().parent
    config = PipelineConfig.from_args(build_parser().parse_args(), base_dir=base_dir)
    logger = configure_logging(config.log_file)

    log_minio_connection(minio_settings, logger=logger)
    minio_client = build_minio_client(minio_settings)

    try:
        for b in minio_client.list_buckets():
            logger.info("Bucket detected: %s", b.name)
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(
            "Unable to list buckets - verify access key/secret pair."
        ) from exc

    logger.info("Step 1/4: validating configuration and preparing directories.")
    step_start = time.perf_counter()
    validate_source(config)
    ensure_directories(config)
    logger.info(
        "⏱️  Configuration validation took %.3f seconds",
        time.perf_counter() - step_start,
    )

    h5_files: List[Tuple[str, Path]] = discover_h5_files(
        config, minio_client=minio_client, minio_settings=minio_settings, logger=logger
    )

    logger.info("Step 3/4: Processing HDF5 files...")
    step3_start = time.perf_counter()
    prepare_postgres_normalized_schema()
    con = setup_duckdb_with_postgres(
        password=os.getenv("POSTGRES_PASSWORD", ""), logger=logger
    )
    ensure_normalized_schema(con)

    for idx, (file_label, file_path) in enumerate(h5_files, 1):
        logger.info("=" * 60)
        logger.info("Processing file %d/%d: %s", idx, len(h5_files), file_label)
        process_file(
            con,
            file_label,
            file_path,
            config,
            minio_client,
            minio_settings,
            logger=logger,
        )

    logger.info(
        "⏱️  Step 3 (all files) took %.3f seconds", time.perf_counter() - step3_start
    )
    logger.info("=" * 60)
    logger.info("🎉 Pipeline completed successfully!")
    logger.info(
        "⏱️  Total pipeline execution took %.3f seconds",
        time.perf_counter() - pipeline_start,
    )


if __name__ == "__main__":
    run_pipeline()
