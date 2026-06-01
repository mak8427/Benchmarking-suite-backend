"""S3-compatible connector utilities for discovery and object download.

The filename is kept for compatibility with older imports. New deployments
should configure GWDG S3 through the `S3_*` environment variables.
"""

from __future__ import annotations

import os
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any, List

from api_backend.storage.minio_client import S3StorageClient, StorageSettings

from analysis_module.utils.common import _mask_secret, _truthy


def _setting(*names: str, default: str | None = None) -> str | None:
    for name in names:
        value = os.getenv(name)
        if value:
            return value
    return default


def _normalize_endpoint_url(value: str | None) -> str:
    endpoint = value or "https://s3.gwdg.de"
    if endpoint.startswith(("http://", "https://")):
        return endpoint.rstrip("/")
    scheme = "https" if _truthy(os.getenv("MINIO_SECURE")) else "http"
    return f"{scheme}://{endpoint.rstrip('/')}"


def resolve_minio_settings() -> dict[str, str | bool]:
    """Collect S3 connection details from environment variables.

    `S3_*` names are preferred. `MINIO_*` and standard AWS names remain aliases
    so older deployments can roll forward without changing every script at once.
    """
    access = _setting("S3_ACCESS_KEY_ID", "AWS_ACCESS_KEY_ID", "MINIO_ACCESS_KEY")
    secret = _setting("S3_SECRET_ACCESS_KEY", "AWS_SECRET_ACCESS_KEY", "MINIO_SECRET_KEY")
    endpoint = _setting(
        "S3_ENDPOINT_URL",
        "AWS_ENDPOINT_URL_S3",
        "AWS_ENDPOINT_URL",
        "MINIO_ADMIN_ENDPOINT",
        "MINIO_PUBLIC_ENDPOINT",
        "MINIO_ENDPOINT",
        default="https://s3.gwdg.de",
    )
    bucket = _setting("S3_BUCKET", "MINIO_BUCKET", default="benchmarking-suite") or "benchmarking-suite"
    prefix = _setting("S3_OBJECT_PREFIX", "MINIO_OBJECT_PREFIX", default="") or ""
    if prefix.startswith("/"):
        prefix = prefix[1:]
    if prefix and not prefix.endswith("/"):
        prefix = f"{prefix}/"

    return {
        "access": access,
        "secret": secret,
        "endpoint": _normalize_endpoint_url(endpoint),
        "bucket": bucket,
        "prefix": prefix,
        "secure": _normalize_endpoint_url(endpoint).startswith("https://"),
        "region": _setting("S3_REGION", "AWS_DEFAULT_REGION", default="us-east-1") or "us-east-1",
    }


def build_minio_client(settings: dict[str, str | bool]) -> S3StorageClient:
    """Construct an S3-compatible client from settings."""
    missing = [name for name in ("endpoint", "access", "secret") if not settings.get(name)]
    if missing:
        raise RuntimeError(
            "Missing S3 configuration for: "
            + ", ".join(missing)
            + ". Ensure S3_ACCESS_KEY_ID, S3_SECRET_ACCESS_KEY, and S3_ENDPOINT_URL "
            "(or compatible MINIO_/AWS_ aliases) are defined."
        )

    return S3StorageClient(
        StorageSettings(
            endpoint_url=str(settings["endpoint"]),
            access_key=str(settings["access"]),
            secret_key=str(settings["secret"]),
            region=str(settings.get("region") or "us-east-1"),
            bucket=str(settings["bucket"]),
        )
    )


def list_minio_objects(client: Any, bucket: str, prefix: str, *, logger) -> List[str]:
    """Return `.h5` object names under a bucket/prefix."""
    objects: List[str] = []
    try:
        for obj in client.list_objects(bucket, prefix=prefix, recursive=True):
            if obj.object_name.endswith(".h5"):
                objects.append(obj.object_name)
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError("Unable to enumerate objects for processing") from exc
    return objects


def download_minio_object(client: Any, bucket: str, object_name: str, *, logger) -> Path:
    """Download an object to a temp file and return its path."""
    tmp = NamedTemporaryFile(delete=False, suffix=".h5")
    tmp.close()
    try:
        client.fget_object(bucket, object_name, tmp.name)
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"Failed to download {bucket}/{object_name}") from exc
    logger.debug("Downloaded %s to %s", object_name, tmp.name)
    return Path(tmp.name)


def log_minio_connection(settings: dict[str, str | bool], *, logger) -> None:
    """Emit basic connection info with masked secrets."""
    logger.info("Connecting to S3 endpoint %s secure=%s", settings["endpoint"], settings["secure"])
    logger.info("Using access key: %s", _mask_secret(settings["access"]))
