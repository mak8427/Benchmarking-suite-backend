# MinIO → Webhook → FastAPI (rootless Podman) — Working Setup Notes

This document records the steps used to reach a working end-to-end flow:

**Object uploaded to MinIO → MinIO sends webhook → FastAPI receives JSON event**

---

## Goal

When an object is uploaded to a MinIO bucket, MinIO should **POST an event payload** to an HTTP endpoint implemented in Python (FastAPI).

MinIO supports bucket notifications via **webhook targets** (`notify_webhook`) and bucket rules configured with `mc event add`.

---

## Starting Point

* MinIO is running inside a **Podman container**
* Podman is running **rootless**, so the MinIO container uses `slirp4netns` networking
* The MinIO client (`mc`) is **not installed on the host**, so it is run as a container image

---

## 1) Inspect the MinIO Container

List running containers:

```bash
podman ps
```

Inspect key properties (name, network mode, ports):

```bash
podman inspect minio --format 'Name={{.Name}}
Network={{.HostConfig.NetworkMode}}
Ports={{.NetworkSettings.Ports}}'
```

Expected result (example):

* `Network=slirp4netns`
* Ports exposed: `9000`, `9001`

**Important ports:**

* `9000` → S3 API (used by `mc`)
* `9001` → Web UI / Console

---

## 2) Run the FastAPI Webhook Receiver

Minimal FastAPI receiver:

```python
from fastapi import FastAPI, Request

app = FastAPI()

@app.post("/minio")
async def minio_event(req: Request):
    payload = await req.json()
    print("MinIO event:", payload)
    return {"ok": True}
```

Run it on the host:

```bash
uvicorn minio_listener:app --host 0.0.0.0 --port 8000
```

Binding to `0.0.0.0` is required so containers can reach it.

---

## 3) Why `host.containers.internal` Matters

In rootless Podman:

* `localhost` inside a container refers to **the container itself**
* To reach services running on the **host**, use:

```text
host.containers.internal
```

This is Podman’s equivalent of Docker’s `host.docker.internal`.

Therefore, the webhook endpoint configured in MinIO must be:

```text
http://host.containers.internal:8000/minio
```

---

## 4) Running `mc` Without Installing It

### Use a Fully-Qualified Image Name

If you see errors like:

> short-name "minio/mc" did not resolve

Use the full image reference:

```text
docker.io/minio/mc
```

This avoids Podman registry resolution issues.

---

## 5) Configure MinIO Webhook + Bucket Event

There are **two required steps**:

1. Define a webhook target (`notify_webhook:<ID>`)
2. Attach bucket events to that webhook using an ARN

---

### 5.1 Configure the Webhook Target (ID = `1`)

First, create an alias pointing to the MinIO S3 API:

```bash
podman run --rm --network=host docker.io/minio/mc \
  alias set myminio http://127.0.0.1:9000 admin <PASSWORD>
```

Then configure the webhook endpoint:

```bash
podman run --rm --network=host --entrypoint /bin/sh docker.io/minio/mc -lc '
  mc alias set myminio http://127.0.0.1:9000 admin <PASSWORD> &&
  mc admin config set myminio/ notify_webhook:1 \
    endpoint="http://host.containers.internal:8000/minio"
'
```

---

### 5.2 Restart MinIO to Apply Configuration

You must restart MinIO after changing notification config.

Either restart via `mc`:

```bash
podman run --rm --network=host --entrypoint /bin/sh docker.io/minio/mc -lc '
  mc alias set myminio http://127.0.0.1:9000 admin <PASSWORD> &&
  mc admin service restart --json --wait myminio/
'
```

Or restart the container directly:

```bash
podman restart minio
```

---

### 5.3 Attach Bucket PUT Events

Attach object upload (`put`) events from bucket `benchwrap` to webhook target `1`:

```bash
podman run --rm --network=host --entrypoint /bin/sh docker.io/minio/mc -lc '
  mc alias set myminio http://127.0.0.1:9000 admin <PASSWORD> &&
  mc event add myminio/benchwrap arn:minio:sqs::1:webhook --event put
'
```

Verify:

```bash
podman run --rm --network=host --entrypoint /bin/sh docker.io/minio/mc -lc '
  mc alias set myminio http://127.0.0.1:9000 admin <PASSWORD> &&
  mc event list myminio/benchwrap
'
```

---

## 6) End-to-End Test

Upload a file:

```bash
podman run --rm --network=host --entrypoint /bin/sh docker.io/minio/mc -lc '
  mc alias set myminio http://127.0.0.1:9000 admin <PASSWORD> &&
  mc cp /etc/hosts myminio/benchwrap/webhook-test-hosts
'
```

Expected result:

* FastAPI receives a POST on `/minio`
* JSON payload contains:

  * `s3:ObjectCreated:Put`
  * bucket name
  * object key

---

## 7) Common Issues and Fixes

### Alias Not Found

Each `podman run --rm` starts fresh.

**Fix:**

* Run alias + commands in the same container
* Or mount a persistent config directory to `/root/.mc`

---

### `sh` Not Recognized

The `mc` image has `mc` as its entrypoint.

**Fix:**
Override entrypoint:

```bash
--entrypoint /bin/sh
```

---

### Invalid or Missing ARN

Webhook ID and ARN **must match**.

If you configure:

```text
notify_webhook:1
```

Then you must use:

```text
arn:minio:sqs::1:webhook
```

---

### Rootless Networking Oddities

Rootless Podman uses `slirp4netns`, which behaves differently from host networking.
Using `--network=host` for `mc` avoids many issues.
