"""Shared pytest fixtures for API tests."""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import AsyncIterator, Iterator

import httpx
import pytest

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

os.environ.setdefault("JWT_SECRET", "test-jwt-secret")

from api_backend import main
from api_backend.db import set_db_path


@pytest.fixture
def anyio_backend() -> str:
    """Force AnyIO to run tests on asyncio backend only.

    This keeps async tests deterministic in local CI where Trio is not installed.

    Returns:
        str: Selected async backend name.

    Examples:
        >>> anyio_backend.__name__
        'anyio_backend'
    """
    return "asyncio"


@pytest.fixture(autouse=True)
def isolate_environment(tmp_path: Path) -> Iterator[None]:
    """Provide isolated filesystem locations and reset dependency overrides.

    Args:
        tmp_path (Path): Temporary directory provided by pytest.

    Yields:
        Iterator[None]: Control back to test body.

    Examples:
        >>> (Path("tmp") / "auth.db").name
        'auth.db'
    """
    db_path = tmp_path / "auth.db"
    set_db_path(db_path)
    main.app.dependency_overrides.clear()
    yield
    main.app.dependency_overrides.clear()


@pytest.fixture
async def client() -> AsyncIterator[httpx.AsyncClient]:
    """Return an async HTTP client bound to the FastAPI ASGI app.

    Yields:
        AsyncIterator[httpx.AsyncClient]: Async API client.

    Examples:
        >>> "testserver" in "http://testserver"
        True
    """
    transport = httpx.ASGITransport(app=main.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as api_client:
        yield api_client
