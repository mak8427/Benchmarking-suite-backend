# Project Overview

## What this repo provides
- **External API** (`main.py`): FastAPI service for auth (register/login/refresh) and MinIO-backed file operations (presigned upload/download, list).
- **Analysis pipeline** (`analysis_module/duckdb_analysis.py` + `analysis_module/pipeline_core/*`): Processes HDF5 datasets into DuckDB/PostgreSQL tables (`job_<h5-stem>`), computes energy metrics, and integrates optional pricing.
- **MinIO event listener** (`analysis_module/minio_listener.py`): FastAPI webhook that reacts to MinIO `.h5` object notifications, runs the analysis pipeline, and writes results to Postgres.
- **Load test tool** (`performance_test/requests.py`): Async HTTP load generator for exercising endpoints.

## Repository layout (key paths)
- `main.py` — public-facing API (auth + storage).
- `storage/minio_client.py` — MinIO clients and bucket constants.
- `util/auth_utils.py` — JWT validation and name sanitization.
- `analysis_module/duckdb_analysis.py` — one-shot pipeline entry; pulls HDF5 (local/MinIO), runs transformations, writes Postgres tables.
- `analysis_module/pipeline_core/` — shared pipeline utilities (data loaders, combining frames, energy/pricing).
- `analysis_module/minio_listener.py` — webhook to process MinIO events.
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
python analysis_module/duckdb_analysis.py
```
Key env vars: `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`, `MINIO_ADMIN_ENDPOINT` (or `MINIO_PUBLIC_ENDPOINT`), `MINIO_BUCKET`, `MINIO_OBJECT_PREFIX`, `MINIO_SECURE`, `MINIO_SYNC=1` (to pull remote files), `POSTGRES_HOST/PORT/DB/USER/PASSWORD`.
Behavior:
- Discovers `.h5` locally and/or in MinIO prefix.
- Processes each into a combined Polars frame, computes energy metrics, and writes `pg.public.job_<h5-stem>` tables via DuckDB’s Postgres extension.
- Logs per-file timing and energy summaries; skips unreadable/empty files with reasons.

## MinIO event-driven analysis
- Service: `analysis_module/minio_listener.py` (FastAPI).
- Start locally: `uvicorn minio_listener:app --host 0.0.0.0 --port 8001`.
- Webhook: `POST /minio-event` expects MinIO bucket notifications payload (filters `.h5`), downloads the object, runs the pipeline, and writes `job_<h5-stem>` to Postgres.
- Health: `GET /healthz`.
- Env: same MinIO/Postgres vars as above.

## Analysis module: detailed behavior

### Event-driven flow (MinIO listener)
1. MinIO emits a bucket notification with `Records[]`.
2. Listener validates the payload and filters for `.h5` keys only.
3. For each `.h5` key, the listener schedules a background task in the same FastAPI process.
4. Background task downloads the object to a temp file.
5. The file is validated (`exists`, non-empty, `h5py.is_hdf5`).
6. `h5_to_dataframe` loads datasets, combines them, and computes energy metrics.
7. DuckDB attaches Postgres and materializes `pg.public.job_<h5-stem>`.
8. Temp file is removed; failures are logged and skipped.

### Batch flow (duckdb_analysis / pipeline_runner)
1. Load MinIO settings and list buckets (sanity check).
2. Validate local source directory (unless `MINIO_SYNC=1`).
3. Collect local `.h5` files and optionally sync remote `.h5` from MinIO.
4. For each file, run the same `h5_to_dataframe` pipeline and write to Postgres.

### Data products
- Primary output: `pg.public.job_<stem>` tables in Postgres.
- CSV exports: the CSV-oriented pipeline in `analysis_module/pipeline_core/pipeline.py` can write per-job CSV, stats, and summary files, but it is not used by the DuckDB runner.

### Metrics computed (per job/group)
The analysis computes the following metrics when a valid power/energy column is present:
- `energy_to_solution_j` (ETS, joules): max of `Energy_used_J`.
- `time_to_solution_s` (TTS, seconds): max of `ElapsedTime`.
- `average_power_w`: `energy_to_solution_j / time_to_solution_s`.
- `peak_power_w`: max `NodePower`.
- `peak_power_time_s`: `ElapsedTime` at peak power.
- `energy_delay_product`: `energy_to_solution_j * time_to_solution_s`.
- `appliance_name`, `appliance_amount`, `appliance_unit`: human-friendly comparison.
- `appliance_description`: text description of the appliance comparison.

### Postgres table format (generated)
Tables are created as `pg.public.job_<h5-stem>` with a dynamic schema derived from the HDF5 contents plus computed columns:
- `ElapsedTime` (UInt64): injected if missing; base timeline for joins.
- `EpochTime` (Int64, optional): added when available from `*__EpochTime` columns.
- Prefixed dataset columns: each dataset column becomes `<prefix>__<column>` where `<prefix>` is the HDF5 path (sanitized and joined by `__`).
- Derived columns (when inputs exist):
  - `NodePower` (Float64): normalized from power columns (`NodePower`, `*__NodePower`, `*__CurrPower`).
  - `ElapsedTime_Diff` (Float64): time deltas used for integration.
  - `Energy_Increment_J` (Float64): incremental energy from power or energy series.
  - `Energy_used_J` (Float64): cumulative energy.
  - `*__RSS_MB`: per-dataset RSS converted to MiB.
  - `*__CPUUtilization_normalized`: CPU utilization normalized by 32.0.
  - `Price_EUR_per_MWh` (Float64, optional): price series from SMARD.
  - `Cumulative_cost_EUR` (Float64, optional): cumulative cost from `Energy_used_J`.

Notes:
- All numeric columns are cast to Float64 during interpolation/casting.
- If no power/energy column is found, energy metrics are skipped and only raw/derived non-energy columns are persisted.

### Table naming and duplicate uploads
- Table name: `job_<h5-stem>` where `<h5-stem>` is derived from the object/file name.
- Duplicate uploads of the same key will overwrite the MinIO object.
- Reprocessing drops and recreates the Postgres table (`DROP TABLE IF EXISTS ...`), so data is replaced, not appended.

### Skip conditions and error handling
- Files are skipped if missing, empty, or not valid HDF5.
- Datasets are skipped if empty, erroring, or contain all-zero `NodePower`.
- If a file yields no usable datasets, it is logged and skipped.
- If Arrow-based registration fails, DuckDB falls back to pandas registration.

### Performance notes
- DuckDB attaches Postgres once per run and reuses the connection.
- HDF5 parsing, frame combination, and Postgres table creation are the main hotspots; per-step timings are logged.

### Analysis configuration (env vars)
- `MINIO_ACCESS_KEY` / `MINIO_SECRET_KEY`: MinIO credentials.
- `MINIO_ADMIN_ENDPOINT` or `MINIO_PUBLIC_ENDPOINT` or `MINIO_ENDPOINT`: MinIO host:port.
- `MINIO_BUCKET`: bucket name (default `benchwrap` in connectors).
- `MINIO_OBJECT_PREFIX`: prefix for `.h5` discovery (default `cane12345/`).
- `MINIO_SECURE`: truthy string to enable HTTPS for MinIO.
- `MINIO_SYNC=1`: enable remote MinIO downloads during batch runs.
- `POSTGRES_HOST` / `POSTGRES_PORT` / `POSTGRES_DB` / `POSTGRES_USER` / `POSTGRES_PASSWORD`: Postgres connection.

## Load testing
```bash
python performance_test/requests.py http://localhost:8000 10000 200
```
Args: `url requests concurrency [processes timeout]` and `--http2`.

## Docker
- Analysis image: `analysis_module/Dockerfile` (runs `duckdb_analysis.py`).
  ```bash
  podman build -f analysis_module/Dockerfile -t duckdb-analysis .
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
- Keep two services: external API (`main.py`) exposed via Ingress; internal listener (`analysis_module/minio_listener.py`) as ClusterIP only.
- MinIO bucket notifications target: `http://analysis-listener-svc.<ns>.svc.cluster.local:8001/minio-event`.
- Store secrets in k8s Secrets; defaults in ConfigMaps.
- Optional CronJob to re-scan bucket/prefix for missed files.

## Testing
- Add pytest suites under `tests/`; run `python -m pytest`.
- `.http` files under `tests/` can be replayed with an HTTP client for manual checks.

## Logs and artifacts
- API logs to `process.log` and stdout.
- Analysis logs to `analysis_module/analysis.log` (or stdout in containers).
- Generated data (e.g., `users.txt`, benchmark data) should not be committed.
