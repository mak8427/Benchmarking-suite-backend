# Benchmarking Suite Backend

This repository contains two Python services:

- `api_backend/` - external FastAPI API for authentication and S3 presign flows.
- `analysis_module/` - batch + event-driven analysis pipeline for `.h5` job artifacts.

## Quickstart

Target Python 3.11+.

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Set required secrets before running:

```bash
export JWT_SECRET='replace-me'
export S3_ENDPOINT_URL='https://s3.gwdg.de'
export S3_BUCKET='benchmarking-suite'
export S3_ACCESS_KEY_ID='...'
export S3_SECRET_ACCESS_KEY='...'
export S3_ADDRESSING_STYLE='path'
```

Legacy `MINIO_*` variables are still accepted as compatibility aliases during
the transition from local MinIO to GWDG S3. See
[`docs/backend_maintenance.md`](docs/backend_maintenance.md) for deployment,
validation, restart, and MinIO cleanup procedures.

## API Backend

Entrypoint: `api_backend/main.py`

```bash
uvicorn api_backend.main:app --reload --host 0.0.0.0 --port 8000
```

Primary endpoints:

- `POST /auth/register`
- `POST /auth/login`
- `POST /auth/refresh`
- `POST /files/presign/upload`
- `GET /files/presign/download`
- `GET /files/list`
- `GET /healthz`

Compatibility endpoints remain available during transition:

- `POST /auth/password`
- `POST /storage/presign/upload`
- `GET /storage/presign/download`
- `GET /storage/list`

## Analysis Module

Batch runner:

```bash
python -m analysis_module.duckdb_analysis
```

Event listener:

```bash
uvicorn analysis_module.minio_listener:app --host 0.0.0.0 --port 8001
```

The listener accepts legacy MinIO-style notifications on:

- `POST /minio-event` (primary)
- `POST /minio` (legacy)

## Config Files

- Analysis config: `analysis_module/pipeline_config.yml`
- Load test config: `api_backend/performance_test/config.yml`

Load test command:

```bash
python -m api_backend.performance_test.requests --config api_backend/performance_test/config.yml
```

Legacy positional invocation is still supported:

```bash
python -m api_backend.performance_test.requests http://localhost:8000/healthz 10000 200
```

## Tests and Checks

Run API-focused tests:

```bash
pytest -q api_backend/tests/test_auth_endpoints.py api_backend/tests/test_storage_endpoints.py api_backend/tests/test_utils.py
```

Run doctests + module collection:

```bash
pytest --doctest-modules
```

Run pre-commit checks:

```bash
pre-commit run --all-files
```

## Security Notes

- `JWT_SECRET` must be provided via environment variable.
- Do not commit credentials, raw `.h5` input data, or generated logs.
- Storage keys are server-generated and scoped to `user_id/<uuid>_<safe_name>`.
