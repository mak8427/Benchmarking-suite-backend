# Deployment Plan (k3s + MinIO)

## Services
- **External API**: `main.py` (auth + presign URLs). Exposed via Ingress/LoadBalancer.
- **Internal Analysis Listener**: `analysis/minio_listener.py` (consumes MinIO `.h5` events, runs pipeline, writes to Postgres). Cluster-internal service only.

## Container Images
- Build separate images:
  - `main` image: starts FastAPI from `main.py`.
  - `analysis-listener` image: starts FastAPI from `analysis/minio_listener.py` (uvicorn on port 8001).

## Kubernetes Objects
- Deployments:
  - `main-api`: replicas sized for client traffic.
  - `analysis-listener`: typically 1–2 replicas (CPU-heavy tasks).
- Services:
  - `main-api-svc`: ClusterIP + Ingress/LoadBalancer for external access.
  - `analysis-listener-svc`: ClusterIP only; MinIO posts notifications here.
- Optional: CronJob to re-scan MinIO nightly in case notifications are missed.

## MinIO Notifications
- Configure bucket notification on the target bucket/prefix for `*.h5` `PUT/CREATE`.
- Target: HTTP POST to `http://analysis-listener-svc.<namespace>.svc.cluster.local:8001/minio-event`.

## Configuration (env/secrets)
- Shared: `MINIO_ENDPOINT`, `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`, `MINIO_SECURE`, `MINIO_BUCKET`, `MINIO_OBJECT_PREFIX`.
- External API: `JWT_SECRET`, `BUCKET_TOKEN_SECRET`, `BUCKET_TOKEN_TTL_MIN`, `MINIO_PUBLIC_ENDPOINT`, `MINIO_ADMIN_ENDPOINT`.
- Analysis Listener: `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`.
- Logging: optional `LOG_FILE_PATH` or rely on stdout.

## Notes
- Listener writes tables as `job_<h5-stem>` using DuckDB’s Postgres extension.
- Keep `analysis-listener` internal; only MinIO should reach it.
- Scale analysis workers cautiously to avoid overloading Postgres/MinIO.

## Example k3s Wiring (suggested)
- **Namespaces**: create a dedicated `bench` namespace for API + listener; keep MinIO either external or in `storage`.
- **Services/Ingress**:
  - `main-api-svc` (ClusterIP) + Ingress to expose externally on `/` (or a subpath/hostname).
  - `analysis-listener-svc` (ClusterIP, no Ingress). MinIO posts to `http://analysis-listener-svc.bench.svc.cluster.local:8001/minio-event`.
- **RBAC/Secrets**:
  - Store MinIO + Postgres credentials in Kubernetes Secrets; mount as env vars in both Deployments.
  - Use a ConfigMap for non-sensitive defaults (prefixes, secure flag).
- **MinIO Notification**:
  - Configure bucket notifications to the listener service URL; filter on `*.h5` in the target prefix.
- **Persistence/Logs**:
  - Prefer stdout for logs; optionally mount a small `emptyDir` if `LOG_FILE_PATH` is needed.
- **Safety Net**:
  - Add a nightly CronJob in `bench` to scan the bucket/prefix for missed files and enqueue/reprocess.
