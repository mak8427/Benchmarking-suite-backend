"""FastAPI webhook listener for MinIO object-created events."""

from __future__ import annotations

import os
import shlex
import subprocess
import time
from typing import Any

from fastapi import BackgroundTasks, FastAPI, Request

app = FastAPI(title="Analysis Listener", version="1.0.0")


def launch_job(bucket: str, key: str) -> None:
    """Launch an analysis job for an uploaded object.

    Args:
        bucket (str): Source bucket name.
        key (str): Uploaded object key.

    Examples:
        >>> import analysis_module.minio_listener as mod
        >>> calls = {}
        >>> def fake_run(command, capture_output, text, check):
        ...     calls["command"] = command
        ...     class Result:
        ...         returncode = 0
        ...         stdout = ""
        ...         stderr = ""
        ...     return Result()
        >>> old_run = mod.subprocess.run
        >>> mod.subprocess.run = fake_run
        >>> mod.launch_job("bench", "sample.h5")  # doctest: +ELLIPSIS
        listener_event=launch_job bucket=bench key=sample.h5 rc=0 cmd=kubectl create job duckdb-...
        >>> calls["command"][:3]
        ['kubectl', 'create', 'job']
        >>> mod.subprocess.run = old_run
    """
    image = os.getenv("ANALYSIS_JOB_IMAGE", "localhost/duckdb-analysis:latest")
    job_name = f"duckdb-{int(time.time())}"
    command = [
        "kubectl",
        "create",
        "job",
        job_name,
        f"--image={image}",
        "--",
        "python",
        "analysis_module/duckdb_analysis.py",
    ]
    result = subprocess.run(command, capture_output=True, text=True, check=False)
    print(
        "listener_event="
        f"launch_job bucket={bucket} key={key} rc={result.returncode} "
        f"cmd={shlex.join(command)}"
    )
    if result.stdout:
        print(result.stdout)
    if result.stderr:
        print(result.stderr)


async def _handle_minio_event(request: Request, background_tasks: BackgroundTasks) -> dict[str, Any]:
    """Parse a MinIO webhook payload and enqueue a job.

    Args:
        request (Request): Incoming webhook request.
        background_tasks (BackgroundTasks): FastAPI background task manager.

    Returns:
        dict[str, Any]: Status payload.

    Examples:
        >>> import asyncio
        >>> class Req:
        ...     async def json(self):
        ...         return {"Records": [{"s3": {"bucket": {"name": "b"}, "object": {"key": "x.h5"}}}]}
        >>> result = asyncio.run(_handle_minio_event(Req(), BackgroundTasks()))
        >>> result["scheduled"]
        1
    """
    payload = await request.json()
    records = payload.get("Records", []) if isinstance(payload, dict) else []
    if not records:
        return {"ok": True, "scheduled": 0}

    scheduled = 0
    for record in records:
        s3 = record.get("s3", {}) if isinstance(record, dict) else {}
        bucket = s3.get("bucket", {}).get("name")
        key = s3.get("object", {}).get("key")
        if not bucket or not key:
            continue
        if not key.endswith(".h5"):
            continue
        background_tasks.add_task(launch_job, bucket, key)
        scheduled += 1

    return {"ok": True, "scheduled": scheduled}


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    """Readiness endpoint for listener service.

    Returns:
        dict[str, str]: Health status payload.

    Examples:
        >>> import asyncio
        >>> asyncio.run(healthz())["status"]
        'ok'
    """
    return {"status": "ok"}


@app.post("/minio-event")
async def minio_event(request: Request, background_tasks: BackgroundTasks) -> dict[str, Any]:
    """Primary MinIO notification endpoint.

    Args:
        request (Request): Incoming request.
        background_tasks (BackgroundTasks): FastAPI background task manager.

    Returns:
        dict[str, Any]: Scheduling summary.

    Examples:
        >>> import asyncio
        >>> class Req:
        ...     async def json(self):
        ...         return {"Records": []}
        >>> asyncio.run(minio_event(Req(), BackgroundTasks()))["scheduled"]
        0
    """
    return await _handle_minio_event(request, background_tasks)


@app.post("/minio")
async def minio_legacy(request: Request, background_tasks: BackgroundTasks) -> dict[str, Any]:
    """Legacy MinIO endpoint kept for backward compatibility.

    Args:
        request (Request): Incoming request.
        background_tasks (BackgroundTasks): FastAPI background task manager.

    Returns:
        dict[str, Any]: Scheduling summary.

    Examples:
        >>> import asyncio
        >>> class Req:
        ...     async def json(self):
        ...         return {"Records": []}
        >>> asyncio.run(minio_legacy(Req(), BackgroundTasks()))["ok"]
        True
    """
    return await _handle_minio_event(request, background_tasks)
