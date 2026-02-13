# Architecture

## Goal

Provide a secure, user-scoped storage API and a pipeline that processes uploaded HPC job `.h5` artifacts into Postgres-ready analytical tables.

## Folder Structure

- `api_backend/`
- `api_backend/main.py`: FastAPI API entrypoint.
- `api_backend/db.py`: SQLite persistence for users, refresh tokens, and jobs.
- `api_backend/util/auth_utils.py`: JWT and object-name validation helpers.
- `api_backend/storage/minio_client.py`: MinIO client wiring.
- `api_backend/performance_test/requests.py`: load generator.
- `analysis_module/`
- `analysis_module/duckdb_analysis.py`: batch analysis entrypoint.
- `analysis_module/minio_listener.py`: MinIO event listener.
- `analysis_module/pipeline_core/`: modular pipeline primitives.
- `analysis_module/connectors/`: MinIO + DB connectors.
- `analysis_module/processing/`: HDF5 transforms.
- `scripts/`: repository quality checks.

## Design Principles

- Modularity: auth API and analysis pipeline remain independently deployable.
- Security by default: user-scoped keys, JWT auth, env-driven secrets.
- Determinism: config-file-first execution for repeatable runs.
- Observability: request IDs and latency logging at HTTP boundaries.

## Data Flow

1. Client registers/logs in through `api_backend`.
2. API issues short-lived access token and rotating refresh token.
3. Client requests upload presign.
4. API returns server-generated key: `user_id/<uuid>_<safe_name>`.
5. File is uploaded to MinIO.
6. MinIO posts notification to `analysis_module/minio_listener.py`.
7. Listener schedules analysis job (`duckdb_analysis.py`).
8. Pipeline reads `.h5`, computes metrics, writes `job_<h5-stem>` table in Postgres.

## Workflow

### Phase 1: API and auth

- Register/login/refresh with persistent token storage.
- User-scoped presign/list endpoints.
- Compatibility routes retained temporarily with deprecation headers.

### Phase 2: Analysis and event handling

- Batch and webhook-driven processing.
- Configured via `analysis_module/pipeline_config.yml`.
- Internal listener endpoint: `/minio-event`.

### Phase 3: Validation and release

- `pre-commit run --all-files`
- `pytest --doctest-modules`
- API test suite under `api_backend/tests/`
