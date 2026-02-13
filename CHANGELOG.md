# Changelog

## [0.0.4] - 2026-02-13
- Description:
  - Improved remote smoke-test base URL discovery, added explicit remote-test opt-in, and added dedicated tests for missing/alternate config locations.
- Files touched:
  - `api_backend/tests/remote/conftest.py`
  - `api_backend/tests/remote/test_remote_base_url_discovery.py`
- Reason:
  - Ensure remote tests can discover `api_backend/http-client.private.env.json` when running from repository root and behave predictably when config is absent.
- Problems fixed:
  - Fixed remote test discovery fallback that ignored `api_backend/http-client.private.env.json`.
  - Prevented default local checks from failing on unstable external endpoints by requiring `RUN_REMOTE_TESTS=1` for live remote execution.
  - Added explicit tests for no-config, `api_backend` config, and env-var precedence behavior.

## [0.0.3] - 2026-02-13
- Description:
  - Fixed ASGI startup/import compatibility so the API can start with both `uvicorn api_backend.main:app` and `uvicorn main:app`.
- Files touched:
  - `api_backend/main.py`
  - `main.py`
- Reason:
  - Prevent runtime `ModuleNotFoundError` caused by invocation-path differences between package and in-folder execution.
- Problems fixed:
  - Resolved startup failure `ModuleNotFoundError: No module named 'api_backend.db'` in environments launching from different working directories.
  - Restored compatibility for root-module startup (`uvicorn main:app`).

## [0.0.2] - 2026-02-13
- Description:
  - Implemented architecture-first compliance pass across API, analysis imports, tests, docs, and pre-commit checks.
- Files touched:
  - `api_backend/main.py`
  - `api_backend/db.py`
  - `api_backend/util/auth_utils.py`
  - `api_backend/storage/minio_client.py`
  - `api_backend/tests/conftest.py`
  - `api_backend/tests/test_auth_endpoints.py`
  - `api_backend/tests/test_storage_endpoints.py`
  - `api_backend/tests/test_utils.py`
  - `api_backend/performance_test/requests.py`
  - `analysis_module/minio_listener.py`
  - `analysis_module/pipeline_core/config.py`
  - `analysis_module/*` import-path updates
  - `.pre-commit-config.yaml`
  - `README.md`
  - `docs/PROJECT_OVERVIEW.md`
  - `ARCHITECTURE.md`
  - `pyproject.toml`
  - `.gitignore`
- Reason:
  - Align implementation with repository Markdown rules: persistent auth state, user-scoped keying, config-file-first scripts, compatibility route strategy, and mandatory test/check commands.
- Problems fixed:
  - Fixed pytest collection/import failures in `analysis_module`.
  - Fixed hanging local tests by migrating to ASGI `httpx.AsyncClient` and simplifying MinIO dependency usage.
  - Added missing pre-commit configuration so mandatory `pre-commit run --all-files` is executable.
  - Repaired doctest examples that referenced invalid paths.
