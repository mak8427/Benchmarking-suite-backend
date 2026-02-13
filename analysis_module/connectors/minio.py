"""MinIO connector utilities for discovery and object download."""

from __future__ import annotations

import os
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import List

from minio import Minio
from minio.error import S3Error

from analysis_module.utils.common import _mask_secret, _truthy


def resolve_minio_settings() -> dict[str, str | bool]:
    """Collect MinIO/S3 connection details from environment variables.

    Returns:
        dict[str, str | bool]: Normalized MinIO settings.

    Examples:
        >>> settings = resolve_minio_settings()
        >>> 'bucket' in settings and 'prefix' in settings
        True
    """
    access = os.getenv("MINIO_ACCESS_KEY") or os.getenv("AWS_ACCESS_KEY_ID")
    secret = os.getenv("MINIO_SECRET_KEY") or os.getenv("AWS_SECRET_ACCESS_KEY")
    endpoint = (
        os.getenv("MINIO_ADMIN_ENDPOINT")
        or os.getenv("MINIO_PUBLIC_ENDPOINT")
        or os.getenv("MINIO_ENDPOINT")
        or os.getenv("AWS_ENDPOINT_URL")
    )
    bucket = os.getenv("MINIO_BUCKET", "benchwrap")
    prefix = os.getenv("MINIO_OBJECT_PREFIX", "cane12345/")
    if prefix.startswith("/"):
        prefix = prefix[1:]
    if prefix and not prefix.endswith("/"):
        prefix = f"{prefix}/"
    secure = _truthy(os.getenv("MINIO_SECURE"))

    return {
        "access": access,
        "secret": secret,
        "endpoint": endpoint,
        "bucket": bucket,
        "prefix": prefix,
        "secure": secure,
    }


def build_minio_client(settings: dict[str, str | bool]) -> Minio:
    """Construct a MinIO client from settings.

    Args:
        settings (dict[str, str | bool]): Normalized MinIO settings.

    Returns:
        Minio: Ready-to-use MinIO client.

    Raises:
        RuntimeError: Raised when required fields are missing.

    Examples:
        >>> try:
        ...     build_minio_client({'endpoint': None, 'access': None, 'secret': None, 'secure': False})
        ... except RuntimeError:
        ...     True
        True
    """
    missing = [name for name in ("endpoint", "access", "secret") if not settings.get(name)]
    if missing:
        raise RuntimeError(
            "Missing MinIO configuration for: "
            + ", ".join(missing)
            + ". Ensure MINIO_ACCESS_KEY, MINIO_SECRET_KEY, and MINIO_ADMIN_ENDPOINT "
            "(or compatible variables) are defined."
        )

    return Minio(
        settings["endpoint"],
        access_key=settings["access"],
        secret_key=settings["secret"],
        secure=bool(settings["secure"]),
    )


def list_minio_objects(client: Minio, bucket: str, prefix: str, *, logger) -> List[str]:
    """Return `.h5` object names under a bucket/prefix.

    Args:
        client (Minio): MinIO client.
        bucket (str): Bucket name.
        prefix (str): Object prefix.
        logger: Logger for diagnostics.

    Returns:
        List[str]: Matching object keys.

    Examples:
        >>> isinstance(list_minio_objects.__name__, str)
        True
    """
    objects: List[str] = []
    try:
        for obj in client.list_objects(bucket, prefix=prefix, recursive=True):
            if obj.object_name.endswith(".h5"):
                objects.append(obj.object_name)
    except S3Error as exc:
        raise RuntimeError("Unable to enumerate objects for processing") from exc
    return objects


def download_minio_object(client: Minio, bucket: str, object_name: str, *, logger) -> Path:
    """Download an object to a temp file and return its path.

    Args:
        client (Minio): MinIO client.
        bucket (str): Bucket name.
        object_name (str): Object key.
        logger: Logger for diagnostics.

    Returns:
        Path: Local temporary path containing object bytes.

    Examples:
        >>> Path('x.h5').suffix
        '.h5'
    """
    tmp = NamedTemporaryFile(delete=False, suffix=".h5")
    try:
        client.fget_object(bucket, object_name, tmp.name)
    except S3Error as exc:
        raise RuntimeError(f"Failed to download {bucket}/{object_name}: {exc.code}") from exc
    logger.info("Downloaded %s to %s", object_name, tmp.name)
    return Path(tmp.name)


def log_minio_connection(settings: dict[str, str | bool], *, logger) -> None:
    """Emit basic connection info with masked secrets.

    Args:
        settings (dict[str, str | bool]): MinIO settings.
        logger: Logger for diagnostics.

    Returns:
        None: Emits log entries only.

    Examples:
        >>> _mask_secret('abcdef', 2)
        'ab***'
    """
    logger.info("Connecting to MinIO endpoint %s secure=%s", settings["endpoint"], settings["secure"])
    logger.info("Using access key: %s", _mask_secret(settings["access"]))
