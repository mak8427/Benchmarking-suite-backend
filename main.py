"""Compatibility ASGI entrypoint for ``uvicorn main:app``."""

from api_backend.main import app

__all__ = ["app"]
