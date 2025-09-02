# Backend with FastAPI + MinIO + DuckDB + PostGres + Grafana

This project provides a secure backend for managing users, authenticating them, 
and giving each user isolated storage space in MinIO (via presigned URLs).
the main goal is to provide a robust and secure way to  create a E2E workflow
for job efficency analysis on HPC clusters using Slurm. 

This is the backend component only; the client (CLI) you can find [here](https://github.com/mak8427/benchmark-suite) 
as a separate repository. 

the web dashboard will be implemented later using Grafana.


---

## TODO Phase 1 (MVP)
* [ ] Implement user registration & login. 
* [ ] Implement MINIO integration.
* [ ] Implement DuckDB and Polars analysis.
* [ ] Implement Postgres for user/token/job metadata.
* [ ] Implement Grafana dashboard.


### Part 1: registration & login. 
* [x] Setup FastAPI project structure.
* [x] Implement user registration & login with JWT.
* [ ] Implement DB schema & migrations instead of plain text
* [ ] Add presign routes for upload/download.
* [ ] Add quotas & lifecycle rules.
* [ ] Add observability (logs + metrics).
* [ ] Harden deployment (TLS, rootless Podman).

### Part 2: MinIO integration.
* [ ] Setup MinIO client in FastAPI.
* [ ] Implement presign logic with user prefixes.


---

## System Design Checklist Phase 1/2

### 1. Architecture

```
Client ──login────────> FastAPI ──JWT──> verify
Client ──upload URL──> FastAPI ──presign PUT──> MinIO
Client ──PUT file────────────────────────────> MinIO
Client ──download URL> FastAPI ──presign GET──> MinIO
FastAPI <──DB──> users, tokens
FastAPI <──Slurm──> submit jobs, poll status
```

### 2. Identity & Auth

* Use a real database (SQLite, Postgres).
* Store users with argon2id password hash.
* Short-lived **access JWT** (5–10 min).
* Rotating **refresh tokens** stored in DB (`jti`, `user_id`, `exp`).
* Secrets loaded from environment variables, not code.

### 3. Authorization Model

* All objects stored under prefix: `bucket/<user_id>/...`.
* Prefix built by server, never by client.
* Listing limited to own prefix.

### 4. Upload/Download

* Presigned URLs (`PUT`/`GET`) valid for 5–10 min.
* Use multipart uploads for large files.
* Filenames sanitized to `[A–Z a–z 0–9 . _ -]`, reject unsafe paths.

### 5. Bucket Layout & Data

* One shared bucket with per-user prefixes.
* Optional versioning if recovery needed.
* Lifecycle rules for cleanup/archiving.
* Server-side encryption enabled.

### 6. Security Hygiene

* TLS for all external traffic.
* Strict CORS allowlist.
* API only handles metadata, not file bytes.
* Audit logs for presign requests.
* Rate limiting per user/IP.

### 7. API Surface

* `POST /auth/register`
* `POST /auth/login`
* `POST /auth/refresh`
* `POST /files/presign/upload`
* `GET /files/presign/download`
* `GET /files/list`
* Optional: job submission/status endpoints.

### 8. CLI integration  

* Jobs submitted with `sbatch`.
* Inputs from MinIO, outputs to `user_id/jobs/{job_id}/...`.
* API tracks job state (`PENDING`, `RUNNING`, `DONE`, `FAILED`).
* Cleanup of scratch outputs.

### 9. Persistence & State

* DB schema:

  ```
  users(id, username, pw_hash, created_at)
  refresh_tokens(jti, user_id, exp, revoked, created_at)
  jobs(id, user_id, slurm_job_id, state, inputs_key, outputs_prefix, created_at)
  ```
* No in-memory token maps; must persist across restarts.

### 10. Observability

* Structured logs (request\_id, user\_id, route, latency).
* Metrics: presign counts, MinIO failures, auth errors.
* Optional tracing.

### 11. Deployment

* Run under Podman rootless containers.
* Non-root user, read-only FS, drop caps.
* Health checks for FastAPI and MinIO.
* TLS termination at ingress proxy.
* Configurable via env vars.

### 12. Quotas & Cleanup

* Per-user quotas on size and object count.
* Enforce on presign request.
* Retention policies for old files.
* GDPR: allow prefix deletion.

### 13. Testing

* Unit tests for auth & prefix logic.
* Integration tests with local MinIO.
* Property tests for filename sanitizer.
* End-to-end: login → presign → upload → download.

### 14. Non-Negotiables

* Access token expiry ≤ 10 min.
* Refresh tokens rotated on every use.
* Keys built as `user_id/uuid4_safeName` to avoid collisions.
* Presigned URLs always scoped to single key + method.

---

