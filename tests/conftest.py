"""Shared pytest fixtures for API tests."""

from __future__ import annotations

import logging
from typing import Iterator
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import main


@pytest.fixture(autouse=True)
def isolate_environment(tmp_path) -> Iterator[None]:
    """Provide isolated filesystem locations and reset global state."""
    original_users = dict(main.USERS)
    original_tokens = dict(main.TOKENS)
    original_users_file = main.USERS_FILE
    original_log_file = main.LOG_FILE

    main.USERS.clear()
    if "alice" in original_users:
        main.USERS["alice"] = original_users["alice"]
    main.TOKENS.clear()

    main.USERS_FILE = tmp_path / "users.txt"
    main.LOG_FILE = tmp_path / "process.log"
    main.USERS_FILE.parent.mkdir(parents=True, exist_ok=True)
    main.USERS_FILE.touch()

    for handler in list(main.LOGGER.handlers):
        if isinstance(handler, logging.FileHandler):
            handler.close()
            main.LOGGER.removeHandler(handler)

    main.app.dependency_overrides.clear()

    yield

    main.USERS.clear()
    main.USERS.update(original_users)
    main.TOKENS.clear()
    main.TOKENS.update(original_tokens)
    main.USERS_FILE = original_users_file
    main.LOG_FILE = original_log_file

    main.app.dependency_overrides.clear()

    for handler in list(main.LOGGER.handlers):
        if isinstance(handler, logging.FileHandler):
            handler.close()
            main.LOGGER.removeHandler(handler)


@pytest.fixture
def client() -> Iterator[TestClient]:
    """Return a test client for the FastAPI application."""
    test_client = TestClient(main.app)
    try:
        yield test_client
    finally:
        test_client.close()
        main.app.dependency_overrides.clear()
