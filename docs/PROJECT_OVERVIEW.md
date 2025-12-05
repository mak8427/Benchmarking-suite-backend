# Project Overview

## What this repo provides
- **External API** (`main.py`): FastAPI service for auth (register/login/refresh) and MinIO-backed file operations (presigned upload/download, list).
- **Analysis pipeline** (`analysis/duckdb_analysis.py` + `analysis_pipeline/*`): Processes HDF5 datasets into DuckDB/PostgreSQL tables (`job_<h5-stem>`), computes energy metrics, and integrates optional pricing.
- **MinIO event listener** (`analysis/minio_listener.py`): FastAPI webhook that reacts to MinIO `.h5` object notifications, runs the analysis pipeline, and writes results to Postgres.
- **Load test tool** (`performance_test/requests.py`): Async HTTP load generator for exercising endpoints.

## Repository layout (key paths)
- `main.py` — public-facing API (auth + storage).
- `storage/minio_client.py` — MinIO clients and bucket constants.
- `util/auth_utils.py` — JWT validation and name sanitization.
- `analysis/duckdb_analysis.py` — one-shot pipeline entry; pulls HDF5 (local/MinIO), runs transformations, writes Postgres tables.
- `analysis/analysis_pipeline/` — shared pipeline utilities (data loaders, combining frames, energy/pricing).
- `analysis/minio_listener.py` — webhook to process MinIO events.
- `performance_test/requests.py` — load generator CLI.
- `docs/` — documentation.

## Setup
Target Python 3.11+.
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

## Running the external API
```bash
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```
Env vars: `JWT_SECRET`, `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`, `MINIO_PUBLIC_ENDPOINT`, `MINIO_ADMIN_ENDPOINT`, `MINIO_BUCKET`, `MINIO_BUCKET_PREFIX`, `BUCKET_TOKEN_SECRET`.
Endpoints:
- `POST /auth/register` — create user, returns access/refresh.
- `POST /auth/password` — login.
- `POST /auth/refresh` — refresh tokens.
- `POST /storage/presign/upload` — presigned PUT for `{user}/{object}`.
- `GET /storage/presign/download` — presigned GET.
- `GET /storage/list` — list objects under `{user}/`.

## Running the analysis pipeline (one-shot)
```bash
python analysis/duckdb_analysis.py
```
Key env vars: `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`, `MINIO_ADMIN_ENDPOINT` (or `MINIO_PUBLIC_ENDPOINT`), `MINIO_BUCKET`, `MINIO_OBJECT_PREFIX`, `MINIO_SECURE`, `MINIO_SYNC=1` (to pull remote files), `POSTGRES_HOST/PORT/DB/USER/PASSWORD`.
Behavior:
- Discovers `.h5` locally and/or in MinIO prefix.
- Processes each into a combined Polars frame, computes energy metrics, and writes `pg.public.job_<h5-stem>` tables via DuckDB’s Postgres extension.
- Logs per-file timing and energy summaries; skips unreadable/empty files with reasons.

## MinIO event-driven analysis
- Service: `analysis/minio_listener.py` (FastAPI).
- Start locally: `uvicorn minio_listener:app --host 0.0.0.0 --port 8001`.
- Webhook: `POST /minio-event` expects MinIO bucket notifications payload (filters `.h5`), downloads the object, runs the pipeline, and writes `job_<h5-stem>` to Postgres.
- Health: `GET /healthz`.
- Env: same MinIO/Postgres vars as above.

## Load testing
```bash
python performance_test/requests.py http://localhost:8000 10000 200
```
Args: `url requests concurrency [processes timeout]` and `--http2`.

## Docker
- Analysis image: `analysis/Dockerfile` (runs `duckdb_analysis.py`).
  ```bash
  podman build -f analysis/Dockerfile -t duckdb-analysis .
  ```
- Run example (host networking for local MinIO/Postgres):
  ```bash
  podman run --rm --network=host \
    -e MINIO_ENDPOINT=host.containers.internal:9000 \
    -e MINIO_ACCESS_KEY=... -e MINIO_SECRET_KEY=... \
    -e MINIO_BUCKET=benchwrap -e MINIO_OBJECT_PREFIX=cane12345/ \
    -e POSTGRES_HOST=... -e POSTGRES_USER=... -e POSTGRES_PASSWORD=... \
    duckdb-analysis
  ```

## k3s deployment (suggested)
- Keep two services: external API (`main.py`) exposed via Ingress; internal listener (`analysis/minio_listener.py`) as ClusterIP only.
- MinIO bucket notifications target: `http://analysis-listener-svc.<ns>.svc.cluster.local:8001/minio-event`.
- Store secrets in k8s Secrets; defaults in ConfigMaps.
- Optional CronJob to re-scan bucket/prefix for missed files.

## Testing
- Add pytest suites under `tests/`; run `python -m pytest`.
- `.http` files under `tests/` can be replayed with an HTTP client for manual checks.

## Logs and artifacts
- API logs to `process.log` and stdout.
- Analysis logs to `analysis/analysis.log` (or stdout in containers).
- Generated data (e.g., `users.txt`, benchmark data) should not be committed.
