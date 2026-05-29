# Backend Maintenance

This document describes how to run and maintain the Benchmarking Suite backend
after migrating storage from local MinIO to GWDG S3.

## Storage Configuration

Preferred environment variables:

```bash
export S3_ENDPOINT_URL="https://s3.gwdg.de"
export S3_BUCKET="benchmarking-suite"
export S3_ACCESS_KEY_ID="..."
export S3_SECRET_ACCESS_KEY="..."
export S3_ADDRESSING_STYLE="path"
export S3_REGION="us-east-1"
```

Compatibility aliases are still accepted during the transition:

```bash
MINIO_ENDPOINT
MINIO_PUBLIC_ENDPOINT
MINIO_ADMIN_ENDPOINT
MINIO_BUCKET
MINIO_ACCESS_KEY
MINIO_SECRET_KEY
AWS_ENDPOINT_URL
AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY
```

Use path-style addressing by default. In live tests, GWDG S3 accepted both
path-style and domain-style requests, but path-style keeps presigned URLs and
Ceph-compatible tooling simpler:

```text
https://s3.gwdg.de/benchmarking-suite
```

## GWDG S3 Validation

GWDG HPC documentation recommends using `https://s3.gwdg.de` for S3 buckets
that are enabled for HPC access. Their documented CLI path uses `rclone` with
provider `Ceph`.

Minimal `rclone` config:

```ini
[gwdg-s3]
type = s3
provider = Ceph
access_key_id = <access key>
secret_access_key = <secret key>
endpoint = https://s3.gwdg.de
acl = private
```

Smoke-test operations:

```bash
rclone lsf gwdg-s3:benchmarking-suite --max-depth 1
printf 'hello\n' > /tmp/s3-smoke.txt
rclone copyto /tmp/s3-smoke.txt gwdg-s3:benchmarking-suite/smoke/s3-smoke.txt
rclone copyto gwdg-s3:benchmarking-suite/smoke/s3-smoke.txt /tmp/s3-smoke.downloaded.txt
cmp /tmp/s3-smoke.txt /tmp/s3-smoke.downloaded.txt
rclone purge gwdg-s3:benchmarking-suite/smoke
```

## Backend Runtime

Required non-storage variables still apply:

```bash
export JWT_SECRET="<stable random value>"
export BUCKET_TOKEN_SECRET="<stable random value>"
export POSTGRES_PASSWORD="<postgres password if analysis uses postgres>"
```

Start the API from the repository root:

```bash
python -m uvicorn api_backend.main:app --host 0.0.0.0 --port 7800
```

If running from `api_backend/`, the compatibility import path still supports:

```bash
python -m uvicorn main:app --host 0.0.0.0 --port 7800
```

Health checks:

```bash
curl http://127.0.0.1:7800/healthz
curl http://127.0.0.1:7800/
```

## Presigned Upload Validation

The CLI sync flow depends on backend-generated presigned URLs:

```text
benchwrap sync -> backend auth -> presigned PUT URL -> direct S3 upload
```

When validating manually, make sure clients send `Content-Length` on PUT
requests. The backend uses boto3 with checksum compatibility settings for GWDG
S3/Ceph to avoid `MissingContentLength` failures.

## Analysis Processing

For S3-backed analysis discovery, prefer:

```bash
export S3_SYNC=1
export S3_OBJECT_PREFIX="<optional-prefix>/"
```

The legacy `MINIO_SYNC=1` and `MINIO_OBJECT_PREFIX` names still work.

GWDG S3 should not be assumed to support local MinIO webhook configuration.
Use a polling job or scheduled scan of `S3_BUCKET/S3_OBJECT_PREFIX` until an
official notification mechanism is selected.

## Restart Procedure

1. Pull the latest code.
2. Install dependencies from `requirements.txt`.
3. Export the S3 and backend secrets.
4. Stop the old API process.
5. Start `uvicorn` again.
6. Run `/healthz` and one presigned upload/download smoke test.

If the service is running inside `tmux`, use `tmux ls` and
`tmux capture-pane` to identify the pane before interrupting/restarting it.

## MinIO Cleanup

Only clean up local MinIO after all of these are true:

- Backend presigned upload/download works against GWDG S3.
- `benchwrap sync` works from the target cluster/login node.
- Analysis can list and download `.h5` files from GWDG S3.
- Any required historical MinIO data has been copied to GWDG S3.

Suggested cleanup on the cloud VM:

```bash
podman stop minio || true
podman rm minio || true
```

Keep the old MinIO data directory until the migration is verified and backed up.
