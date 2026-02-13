# Project Overview

## What this repo provides

- `api_backend/main.py`: external FastAPI API for auth and user-scoped MinIO presign/list operations.
- `analysis_module/duckdb_analysis.py`: batch pipeline entrypoint for local/remote `.h5` processing.
- `analysis_module/minio_listener.py`: internal FastAPI webhook listener for MinIO notifications.
- `api_backend/performance_test/requests.py`: async load generator with YAML config support.

## Repository layout

- `api_backend/`: auth, storage API, tests, and load tools.
- `analysis_module/`: pipeline core, data processing, MinIO/Postgres connectors.
- `scripts/`: repository validation scripts (docstrings, doctest ratio, file length).
- `docs/`: architecture and operational notes.

## API backend run

```bash
uvicorn api_backend.main:app --reload --host 0.0.0.0 --port 8000
```

Required env vars:

- `JWT_SECRET`
- `MINIO_ACCESS_KEY`
- `MINIO_SECRET_KEY`

Common env vars:

- `MINIO_PUBLIC_ENDPOINT`, `MINIO_ADMIN_ENDPOINT`, `MINIO_BUCKET`
- `AUTH_DB_PATH`
- `MAX_OBJECTS_PER_USER`, `MAX_STORAGE_BYTES_PER_USER`

## Analysis run

Batch:

```bash
python -m analysis_module.duckdb_analysis
```

Listener:

```bash
uvicorn analysis_module.minio_listener:app --host 0.0.0.0 --port 8001
```

Listener endpoints:

- `POST /minio-event` (primary)
- `POST /minio` (legacy compatibility)
- `GET /healthz`

## Config-driven execution

- Analysis config file: `analysis_module/pipeline_config.yml`
- Load test config file: `api_backend/performance_test/config.yml`

Examples:

```bash
python -m api_backend.performance_test.requests --config api_backend/performance_test/config.yml
python -m analysis_module.duckdb_analysis
```

## Testing

```bash
pytest --doctest-modules
pytest -q api_backend/tests/test_auth_endpoints.py api_backend/tests/test_storage_endpoints.py api_backend/tests/test_utils.py
```

## Logging and artifacts

- API logs: `process.log` (configurable via `LOG_FILE_PATH`).
- Analysis logs: `analysis_module/analysis.log`.
- Do not commit credentials, generated logs, or raw benchmark datasets.
