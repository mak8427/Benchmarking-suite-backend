# Benchmarking Suite Backend Maintainer Guide

This repository contains the service side of the Benchmarking Suite. It exposes the authenticated API used by the CLI, stores uploaded benchmark artifacts in GWDG S3, processes Slurm profiling HDF5 files into PostgreSQL, and provisions per-user Grafana workspaces with row-level access control.

## System Responsibilities

The backend has four main responsibilities:

1. Authenticate users and issue access/refresh tokens.
2. Issue private presigned S3 upload and download URLs under a per-user object prefix.
3. Normalize uploaded Slurm HDF5 profiling files into PostgreSQL tables used by Grafana.
4. Provision one Grafana organization, one read-only PostgreSQL role, one datasource, and one dashboard per backend user.

The current production deployment runs on the GWDG cloud instance `gwdg-gloud`. The public API is routed through Traefik at `/api`, and Grafana is served at `/grafana` with the standard Grafana login screen.

## Repository Map

- `api_backend/main.py`: FastAPI routes for auth, token refresh, storage presign, storage listing, and health checks.
- `api_backend/db.py`: SQLite persistence for backend users, refresh tokens, job records, storage object ownership, and Grafana/Postgres workspace credentials.
- `api_backend/grafana.py`: Grafana provisioning and dashboard JSON generation.
- `api_backend/storage/minio_client.py`: S3-compatible client wrapper. The historical MinIO naming remains in imports, but production targets GWDG S3.
- `analysis_module/duckdb_analysis.py`: Main batch entry point for HDF5 discovery, parsing, Postgres materialization, and normalized dashboard writes.
- `analysis_module/connectors/minio.py`: S3 discovery and download helpers.
- `analysis_module/connectors/normalized.py`: Normalized `benchmark_jobs` and `benchmark_samples` writers plus RLS setup.
- `analysis_module/pipeline_core/energy_profile.py`: Energy integration and invalid elapsed-time filtering.
- `api_backend/tests/`: Local unit and integration tests.
- `api_backend/tests/remote/`: Optional remote smoke tests against a deployed API.

## Runtime Data Flow

```text
benchwrap sync
  -> POST /api/storage/presign/upload?object_name=...&benchmark_name=...
  -> backend records object ownership in SQLite
  -> backend returns private GWDG S3 presigned PUT URL
  -> CLI uploads file directly to S3
  -> analysis pipeline lists user prefix in S3
  -> HDF5 files are downloaded to temp files
  -> Polars parses and cleans time/power/energy samples
  -> DuckDB materializes raw per-file tables in PostgreSQL
  -> normalized benchmark_jobs / benchmark_samples are upserted
  -> Grafana queries normalized tables through a per-user Postgres role
```

## Configuration

The deployment uses environment variables, usually sourced from `.env` on the cloud instance. Required values are:

- `JWT_SECRET`: HMAC secret for API access and refresh tokens. Use at least 32 random bytes.
- `S3_ENDPOINT_URL`: GWDG S3 endpoint, normally `https://s3.gwdg.de`.
- `S3_ACCESS_KEY_ID`: GWDG S3 access key.
- `S3_SECRET_ACCESS_KEY`: GWDG S3 secret key.
- `S3_BUCKET`: Bucket name, currently `benchmarking-suite`.
- `S3_ADDRESSING_STYLE`: Usually `path` for GWDG S3 compatibility.
- `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`: PostgreSQL connection used by DuckDB, Grafana provisioning, and RLS setup.
- `GRAFANA_URL`: Internal Grafana URL, normally `http://127.0.0.1:3000`.
- `GRAFANA_ADMIN_USER`, `GRAFANA_ADMIN_PASSWORD`: Grafana admin credentials used only for provisioning.
- `AUTH_DB_PATH`: Optional path to the SQLite auth database. Defaults to `auth.db` in the working directory.
- `MAX_STORAGE_BYTES_PER_USER`: Optional per-user S3 quota. Defaults to 10 GiB.

Do not commit secrets. The local operations secrets are tracked outside the repos under `/home/davidem/PycharmProjects/.secrets/benchmarking-suite/`.

## Local Development

From the repository root:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .
pytest api_backend/tests/test_auth_endpoints.py api_backend/tests/test_storage_endpoints.py api_backend/tests/test_normalized_dashboard.py
```

Run the API locally:

```bash
source .env
python -m uvicorn main:app --host 0.0.0.0 --port 7800
```

The cloud deployment currently starts the app from the repository root with the same command inside tmux.

## Deployment Runbook

Update the cloud instance after pushing to both remotes:

```bash
ssh gwdg-gloud 'cd ~/Benchmarking-suite-backend && git pull --ff-only origin master'
ssh gwdg-gloud 'cd ~/Benchmarking-suite-backend && tmux send-keys -t 0:0.2 C-c'
ssh gwdg-gloud 'cd ~/Benchmarking-suite-backend && tmux send-keys -t 0:0.2 "source .env; python -m uvicorn main:app --host 0.0.0.0 --port 7800" Enter'
ssh gwdg-gloud 'curl -fsS http://127.0.0.1:7800/healthz'
```

The expected health response is `{"status":"ok"}`.

When provisioning changes are made, reprovision the affected user:

```bash
ssh gwdg-gloud 'fish -c "cd ~/Benchmarking-suite-backend; source .env; python -"' <<'PY'
from api_backend.db import get_user_by_username
from api_backend.grafana import GrafanaProvisioner
user = get_user_by_username("USERNAME")
GrafanaProvisioner().provision_user(user_id=user["id"], username=user["username"])
PY
```

## Processing S3 Uploads

The full processing pass for one user prefix is:

```bash
ssh gwdg-gloud "cd ~/Benchmarking-suite-backend && fish -c 'source .env; set -x S3_SYNC 1; set -x S3_OBJECT_PREFIX USER_ID; python -m analysis_module.duckdb_analysis --allow-missing-source'"
```

Important behavior:

- `S3_OBJECT_PREFIX` should be a backend user id, not a username.
- The pipeline downloads each HDF5 object to a temp file, parses it, and writes raw and normalized PostgreSQL rows.
- `prepare_postgres_normalized_schema()` must run before DuckDB attaches to PostgreSQL, otherwise DuckDB can cache an old table schema.
- Per-object S3 download logging is debug-level; normal runs should not print every download.
- A full repair pass over hundreds of HDF5 files is acceptable but should not be the normal sync path long term. Normal sync should eventually process only newly uploaded objects.

## Normalized Database Model

`benchmark_jobs` is one row per uploaded HDF5 object. Key columns:

- `object_key`: S3 object key and primary key.
- `owner_user_id`, `owner_username`: ownership metadata copied from SQLite.
- `original_filename`: filename before UUID prefixing.
- `benchmark_name`: supplied by the CLI during sync. Old uploads may be `unknown`.
- `processed_at`: backend processing time.
- `measured_at`: first sample timestamp from the HDF5 data. Grafana should use this as the job time.
- `sample_count`, `max_power_w`, `mean_power_w`, `total_energy_j`, `max_elapsed_time_s`.
- `job_id`, `compute_node`: derived from Slurm profile filenames such as `14001040_batch_agq007.h5`.

`benchmark_samples` stores time-series rows per object. Grafana uses this for plots.

RLS is enabled on both normalized tables. Per-user Grafana roles receive `SELECT` only and can see rows where `owner_user_id = current_setting('app.user_id', true)`. The Grafana datasource sets that session variable for the user's private role.

## Slurm HDF5 Files: Batch vs Step 0

Slurm profiling commonly emits both files for the same job:

- `JOBID_batch_NODE.h5`: the batch script step.
- `JOBID_0_NODE.h5`: job step `0`, usually created by the actual workload step.

In observed data these files have almost identical epoch ranges, power values, and total energy. They are not independent jobs. Dashboard job-level stats therefore select one canonical row per Slurm job, benchmark, and compute node, preferring `_batch_` when present and otherwise falling back to the longest elapsed row. Summing `_batch_` and `_0_` would double-count energy and cluster time.

Keep raw per-file rows for diagnostics. Aggregate only in Grafana SQL or in a future materialized job-summary table.

## Grafana Behavior

Registration provisions:

- a Grafana user with the same username and password as the backend account,
- a private Grafana org,
- a per-user PostgreSQL datasource,
- a starter dashboard.

Users are Editors in their own private org so they can edit panels and queries, but the datasource role is read-only and RLS-scoped to their own rows.

Dashboard design:

- `Processed Jobs`: count of canonical job rows after the dashboard benchmark and time filters are applied.
- `Total Energy`: sum over canonical per-job rows, not all files.
- `Cluster Time`: sum over canonical per-job elapsed time, not all files.
- `Recent Jobs`: benchmark measurement time, job id, benchmark name, node, selected canonical file, sample count, max power, mean power, total energy, elapsed time.
- `Mean Energy by Compute Node`, `Mean Power by Compute Node`, and `Mean Elapsed Time by Compute Node`: per-node comparisons across all benchmarks or the selected benchmark variable.
- `Compute Node Benchmark Summary`: grouped table for benchmark, node, run count, mean energy, mean power, and mean elapsed time.

If a dashboard shows `unknown` benchmark names, check whether the upload was created before CLI metadata support or whether the HDF5 file cannot be mapped to a Benchwrap job folder.

## Security Notes

- S3 objects are private. Users receive presigned PUT/GET URLs only for keys under their backend user id prefix.
- The backend admin S3 key can list all objects and should live only on the server.
- Grafana users should not share an org. One org per backend user keeps accidental dashboard sharing low-risk.
- Grafana datasource roles should remain read-only. Do not grant write privileges to user roles.
- PostgreSQL RLS is the real data isolation boundary for Grafana SQL access.
- Rotate any credentials that were exposed in terminal history or chat.

## Common Checks

Verify API health:

```bash
curl -fsS http://127.0.0.1:7800/healthz
```

Check normalized job summary:

```sql
select owner_username, count(*) files, count(distinct job_id) jobs,
       sum(total_energy_j) total_energy_j,
       sum(max_elapsed_time_s) file_elapsed_s
from benchmark_jobs
group by owner_username;
```

Check for impossible energy outliers:

```sql
select original_filename, total_energy_j, max_elapsed_time_s
from benchmark_jobs
where total_energy_j > 1e12
order by total_energy_j desc;
```

Check canonical dashboard rows:

```sql
with ranked as (
  select *, row_number() over (
    partition by job_id
    order by case when original_filename like '%_batch_%' then 0 else 1 end,
             max_elapsed_time_s desc
  ) as rn
  from benchmark_jobs
)
select measured_at, job_id, benchmark_name, compute_node, original_filename,
       sample_count, max_power_w, mean_power_w, total_energy_j, max_elapsed_time_s
from ranked
where rn = 1
order by measured_at desc nulls last;
```

## Known Maintenance Tasks

- Incremental processing: avoid full-prefix reprocessing during normal sync.
- Backfill benchmark names for old uploads if a reliable Slurm job id to benchmark mapping is available.
- Consider a materialized `benchmark_job_summaries` table for canonical per-job stats instead of repeating ranking SQL in Grafana.
- Keep IO500 excluded from benchmark correctness work until it is explicitly reintroduced.
- Replace FastAPI `on_event` startup hooks with lifespan handlers when touching application startup.
- Review `analysis_module/Dockerfile` and `analysis_module/duckdb-job.yaml` before using Kubernetes processing; they are older than the current `analysis_module.duckdb_analysis` batch entry point.
