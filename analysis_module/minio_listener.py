from __future__ import annotations

import logging
import os
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Dict, List, Tuple

import duckdb
from fastapi import BackgroundTasks, FastAPI, HTTPException
from minio import Minio
from minio.error import S3Error

from pipeline_core import PipelineConfig, build_parser, configure_logging
from pipeline_core.data_loader import sanitize_parts
from analysis.h5_processing import HDF5OpenError, h5_to_dataframe
from analysis.utils.common import validate_h5_file
from connectors.minio import resolve_minio_settings

BASE_DIR = Path(__file__).resolve().parent
CONFIG = PipelineConfig.from_args(build_parser().parse_args([]), base_dir=BASE_DIR)
LOGGER = configure_logging(CONFIG.log_file)


def _postgres_conn_str() -> str:
    host = os.getenv("POSTGRES_HOST", "127.0.0.1")
    port = int(os.getenv("POSTGRES_PORT", "5432"))
    dbname = os.getenv("POSTGRES_DB", "postgres")
    user = os.getenv("POSTGRES_USER", "postgres")
    password = os.getenv("POSTGRES_PASSWORD", "")
    return f"host={host} port={port} dbname={dbname} user={user} password={password}"


def _setup_duckdb_connection(logger: logging.Logger) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("INSTALL postgres;")
    con.execute("LOAD postgres;")
    conn_str = _postgres_conn_str()
    con.execute(f"ATTACH '{conn_str}' AS pg (TYPE postgres);")
    logger.info("Attached DuckDB to PostgreSQL at %s", conn_str)
    return con


def _download_minio_object(client: Minio, bucket: str, object_name: str) -> Path:
    tmp = NamedTemporaryFile(delete=False, suffix=".h5")
    try:
        client.fget_object(bucket, object_name, tmp.name)
    except S3Error as exc:
        raise RuntimeError(f"Failed to download {bucket}/{object_name}: {exc.code}") from exc
    return Path(tmp.name)


def _process_object(bucket: str, object_name: str, *, client: Minio, logger: logging.Logger) -> Dict[str, str]:
    logger.info("Processing MinIO object %s/%s", bucket, object_name)
    temp_path = _download_minio_object(client, bucket, object_name)

    if not validate_h5_file(temp_path, logger=logger):
        raise RuntimeError(f"Downloaded file is not a valid HDF5: {object_name}")

    table_suffix = sanitize_parts([Path(object_name).stem])
    table_name = f"job_{table_suffix}"
    df_name = f"dataframe_{table_suffix}"

    con = _setup_duckdb_connection(logger)
    try:
        dataframe = h5_to_dataframe(
            temp_path,
            config=CONFIG,
            logger=logger,
            display_name=Path(object_name).stem,
        )
        if dataframe is None or dataframe.is_empty():
            raise RuntimeError(f"No usable data produced for {object_name}")

        con.register(df_name, dataframe)
        con.execute(f"DROP TABLE IF EXISTS pg.public.{table_name};")
        con.execute(f"CREATE TABLE pg.public.{table_name} AS SELECT * FROM {df_name};")
        logger.info("Created PostgreSQL table %s from %s", table_name, object_name)
    finally:
        con.close()
        try:
            temp_path.unlink()
        except OSError:
            logger.warning("Could not delete temp file %s", temp_path)

    return {"bucket": bucket, "object": object_name, "table": table_name}


def _build_minio_client() -> Minio:
    settings = resolve_minio_settings()
    missing = [name for name in ("endpoint", "access", "secret") if not settings.get(name)]
    if missing:
        raise RuntimeError("Missing MinIO settings: " + ", ".join(missing))
    return Minio(
        settings["endpoint"],
        access_key=settings["access"],
        secret_key=settings["secret"],
        secure=bool(settings["secure"]),
    )


app = FastAPI(title="MinIO HDF5 Listener", version="0.1.0")
MINIO_CLIENT = _build_minio_client()


@app.get("/healthz")
def health() -> Dict[str, str]:
    return {"status": "ok"}


@app.post("/minio-event")
async def minio_event(payload: Dict, background_tasks: BackgroundTasks) -> Dict[str, object]:
    records: List[Dict] = payload.get("Records") or []
    if not records:
        raise HTTPException(status_code=400, detail="No Records found in payload")

    accepted: List[Tuple[str, str]] = []
    for record in records:
        s3_info = record.get("s3", {})
        bucket = s3_info.get("bucket", {}).get("name")
        object_name = s3_info.get("object", {}).get("key")
        if not bucket or not object_name:
            LOGGER.warning("Skipping record with missing bucket/key: %s", record)
            continue
        if not object_name.endswith(".h5"):
            LOGGER.info("Ignoring non-h5 object: %s", object_name)
            continue

        accepted.append((bucket, object_name))
        background_tasks.add_task(_process_object, bucket, object_name, client=MINIO_CLIENT, logger=LOGGER)

    if not accepted:
        raise HTTPException(status_code=400, detail="No .h5 objects to process")

    return {
        "accepted": len(accepted),
        "objects": [{"bucket": b, "object": o} for b, o in accepted],
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8001")))
