# Benchmarking Suite Backend

This repo is split into two modules:

- **api_backend/** – External-facing FastAPI service for auth and MinIO-backed file operations.
- **analysis_module/** – Batch and event-driven pipeline that ingests HDF5 data, computes energy metrics, and writes results to Postgres via DuckDB.

## Quickstart

### API Backend
- Entrypoint: `api_backend/main.py`
- Run locally:
  ```bash
  uvicorn api_backend.main:app --reload --host 0.0.0.0 --port 8000
  ```
- Key env vars: `JWT_SECRET`, `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`, `MINIO_PUBLIC_ENDPOINT`, `MINIO_ADMIN_ENDPOINT`, `MINIO_BUCKET`, `MINIO_BUCKET_PREFIX`, `BUCKET_TOKEN_SECRET`.
- Endpoints (examples):
  - `POST /auth/register` / `POST /auth/password` / `POST /auth/refresh`
  - `POST /storage/presign/upload`
  - `GET /storage/presign/download`
  - `GET /storage/list`

### Analysis Module
- Batch runner: `analysis_module/duckdb_analysis.py` → calls `pipeline_runner.run_pipeline()`.
  ```bash
  python analysis_module/duckdb_analysis.py
  ```
- Event listener: `analysis_module/minio_listener.py` (FastAPI webhook).
  ```bash
  uvicorn analysis_module.minio_listener:app --host 0.0.0.0 --port 8001
  ```
- Core packages:
  - `analysis_module/pipeline_core/` – config, loaders, energy/pricing, combiner, discovery.
  - `analysis_module/processing/` – HDF5 parsing and casting.
  - `analysis_module/connectors/` – MinIO, DuckDB/Postgres, file discovery.
  - `analysis_module/utils/` – shared helpers.
- Key env vars: `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`, `MINIO_ENDPOINT`/`MINIO_ADMIN_ENDPOINT`, `MINIO_BUCKET`, `MINIO_OBJECT_PREFIX`, `MINIO_SECURE`, `MINIO_SYNC=1` (to pull remote files), `POSTGRES_HOST/PORT/DB/USER/PASSWORD`.
- Tables are written to Postgres as `pg.public.job_<h5-stem>`.

## Docker
- Analysis image: `analysis_module/Dockerfile` (runs `duckdb_analysis.py`).
  ```bash
  podman build -f analysis_module/Dockerfile -t duckdb-analysis .
  podman run --rm --network=host \
    -e MINIO_ENDPOINT=host.containers.internal:9000 \
    -e MINIO_ACCESS_KEY=... -e MINIO_SECRET_KEY=... \
    -e MINIO_BUCKET=... -e MINIO_OBJECT_PREFIX=... \
    -e POSTGRES_HOST=... -e POSTGRES_USER=... -e POSTGRES_PASSWORD=... \
    duckdb-analysis
  ```
- API image: build from repo root (uses `api_backend/main.py`).
  ```bash
  podman build -t api-backend .
  podman run --rm --network=host -e JWT_SECRET=... -e MINIO_ACCESS_KEY=... -e MINIO_SECRET_KEY=... api-backend
  ```

## k3s suggestion
- Keep two Deployments/Services: `api_backend` exposed via Ingress; `analysis_listener` as ClusterIP only.
- MinIO bucket notifications target: `http://analysis-listener-svc.<ns>.svc.cluster.local:8001/minio-event`.
- Secrets via k8s Secrets; non-sensitive defaults via ConfigMaps.
- Optional CronJob to re-scan MinIO for missed files.

## Testing
- API tests under `api_backend/tests/`; run with `python -m pytest api_backend/tests`.
- Add analysis tests under `analysis_module/tests/` (none present yet).

## Logging / artifacts
- API logs to `process.log` + stdout.
- Analysis logs to `analysis_module/analysis.log` (or stdout in containers).
- Avoid committing credentials or generated artifacts (e.g., populated `users.txt`, raw MinIO data).
