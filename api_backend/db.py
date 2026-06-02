"""SQLite persistence for users, refresh tokens, and jobs."""

from __future__ import annotations

import os
import secrets
import sqlite3
import threading
import time
import uuid
from pathlib import Path
from typing import Any

DB_PATH = Path(os.getenv("AUTH_DB_PATH", "auth.db"))
_DB_LOCK = threading.Lock()


def set_db_path(path: Path) -> None:
    """Set runtime database path and initialize schema.

    Args:
        path (Path): New SQLite file path.

    Examples:
        >>> tmp = Path('/tmp/example-auth.db')
        >>> tmp.name.endswith('.db')
        True
    """
    global DB_PATH
    DB_PATH = path
    init_db()


def _connect() -> sqlite3.Connection:
    """Create a SQLite connection with row mappings.

    Returns:
        sqlite3.Connection: Open database connection.

    Examples:
        >>> conn = _connect()
        >>> isinstance(conn, sqlite3.Connection)
        True
        >>> conn.close()
    """
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(DB_PATH, check_same_thread=False)
    connection.row_factory = sqlite3.Row
    return connection


def init_db() -> None:
    """Create required auth and job tables when missing.

    This operation is idempotent and safe to call on startup.

    Examples:
        >>> init_db()
        >>> DB_PATH.exists()
        True
    """
    with _DB_LOCK:
        with _connect() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS users (
                    id TEXT PRIMARY KEY,
                    username TEXT NOT NULL UNIQUE,
                    pw_hash TEXT NOT NULL,
                    created_at REAL NOT NULL
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS refresh_tokens (
                    jti TEXT PRIMARY KEY,
                    user_id TEXT NOT NULL,
                    exp REAL NOT NULL,
                    revoked INTEGER NOT NULL DEFAULT 0,
                    created_at REAL NOT NULL,
                    FOREIGN KEY(user_id) REFERENCES users(id)
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS jobs (
                    id TEXT PRIMARY KEY,
                    user_id TEXT NOT NULL,
                    slurm_job_id TEXT,
                    state TEXT NOT NULL,
                    inputs_key TEXT,
                    outputs_prefix TEXT,
                    created_at REAL NOT NULL,
                    FOREIGN KEY(user_id) REFERENCES users(id)
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS storage_objects (
                    object_key TEXT PRIMARY KEY,
                    user_id TEXT NOT NULL,
                    username TEXT NOT NULL,
                    original_filename TEXT NOT NULL,
                    benchmark_name TEXT,
                    state TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    uploaded_at REAL,
                    processed_at REAL,
                    FOREIGN KEY(user_id) REFERENCES users(id)
                )
                """
            )
            try:
                connection.execute("ALTER TABLE storage_objects ADD COLUMN benchmark_name TEXT")
            except sqlite3.OperationalError:
                pass
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS user_workspaces (
                    user_id TEXT PRIMARY KEY,
                    grafana_org_id INTEGER,
                    postgres_role TEXT NOT NULL,
                    postgres_password TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    FOREIGN KEY(user_id) REFERENCES users(id)
                )
                """
            )
            connection.commit()


def create_user(username: str, password_hash: str) -> dict[str, Any]:
    """Create and persist a new user record.

    Args:
        username (str): Username chosen by caller.
        password_hash (str): Argon2 password hash.

    Returns:
        dict[str, Any]: Inserted user payload.

    Examples:
        >>> init_db()
        >>> name = f"doc_{uuid.uuid4().hex[:6]}"
        >>> user = create_user(name, 'hash123')
        >>> user['username'] == name
        True
    """
    user_id = str(uuid.uuid4())
    created_at = time.time()
    with _DB_LOCK:
        with _connect() as connection:
            connection.execute(
                "INSERT INTO users(id, username, pw_hash, created_at) VALUES (?, ?, ?, ?)",
                (user_id, username, password_hash, created_at),
            )
            connection.commit()
    return {
        "id": user_id,
        "username": username,
        "pw_hash": password_hash,
        "created_at": created_at,
    }


def get_user_by_username(username: str) -> dict[str, Any] | None:
    """Load a user record by username.

    Args:
        username (str): Username lookup key.

    Returns:
        dict[str, Any] | None: User row when found.

    Examples:
        >>> init_db()
        >>> found = get_user_by_username('missing_user_example')
        >>> found is None
        True
    """
    with _connect() as connection:
        row = connection.execute(
            "SELECT id, username, pw_hash, created_at FROM users WHERE username = ?",
            (username,),
        ).fetchone()
    if row is None:
        return None
    return dict(row)


def get_user_by_id(user_id: str) -> dict[str, Any] | None:
    """Load a user record by identifier.

    Args:
        user_id (str): User identifier.

    Returns:
        dict[str, Any] | None: User row when found.

    Examples:
        >>> init_db()
        >>> get_user_by_id('missing-user-id') is None
        True
    """
    with _connect() as connection:
        row = connection.execute(
            "SELECT id, username, pw_hash, created_at FROM users WHERE id = ?",
            (user_id,),
        ).fetchone()
    if row is None:
        return None
    return dict(row)


def list_users() -> list[dict[str, Any]]:
    """Return all backend users ordered by creation time."""
    with _connect() as connection:
        rows = connection.execute(
            "SELECT id, username, pw_hash, created_at FROM users ORDER BY created_at, username"
        ).fetchall()
    return [dict(row) for row in rows]


def record_storage_object(
    *,
    object_key: str,
    user_id: str,
    username: str,
    original_filename: str,
    benchmark_name: str | None = None,
    state: str = "presigned",
) -> None:
    """Record ownership metadata for an object key.

    Args:
        object_key (str): S3 object key.
        user_id (str): Backend user identifier.
        username (str): Backend username.
        original_filename (str): Client-supplied filename before UUID prefixing.
        benchmark_name (str | None): Optional benchmark name supplied by the CLI.
        state (str): Upload lifecycle state.
    """
    with _DB_LOCK:
        with _connect() as connection:
            try:
                connection.execute("ALTER TABLE storage_objects ADD COLUMN benchmark_name TEXT")
            except sqlite3.OperationalError:
                pass
            connection.execute(
                (
                    "INSERT INTO storage_objects "
                    "(object_key, user_id, username, original_filename, benchmark_name, state, created_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?) "
                    "ON CONFLICT(object_key) DO UPDATE SET "
                    "user_id=excluded.user_id, username=excluded.username, "
                    "original_filename=excluded.original_filename, "
                    "benchmark_name=excluded.benchmark_name, state=excluded.state"
                ),
                (object_key, user_id, username, original_filename, benchmark_name, state, time.time()),
            )
            connection.commit()


def get_storage_object(object_key: str) -> dict[str, Any] | None:
    """Return stored ownership metadata for an object key."""
    with _connect() as connection:
        row = connection.execute(
            (
                "SELECT object_key, user_id, username, original_filename, benchmark_name, state, "
                "created_at, uploaded_at, processed_at FROM storage_objects WHERE object_key = ?"
            ),
            (object_key,),
        ).fetchone()
    if row is None:
        return None
    return dict(row)


def mark_storage_object_uploaded(object_key: str) -> None:
    """Mark an object as uploaded after it is observed in storage."""
    with _DB_LOCK:
        with _connect() as connection:
            connection.execute(
                "UPDATE storage_objects SET state = 'uploaded', uploaded_at = ? WHERE object_key = ?",
                (time.time(), object_key),
            )
            connection.commit()


def mark_storage_object_processed(object_key: str) -> None:
    """Mark an object as processed by the analysis pipeline."""
    with _DB_LOCK:
        with _connect() as connection:
            connection.execute(
                "UPDATE storage_objects SET state = 'processed', processed_at = ? WHERE object_key = ?",
                (time.time(), object_key),
            )
            connection.commit()


def get_or_create_user_workspace(user_id: str) -> dict[str, Any]:
    """Return per-user Grafana/Postgres workspace secrets."""
    role = f"bench_user_{user_id.replace('-', '_')}"
    with _DB_LOCK:
        with _connect() as connection:
            row = connection.execute(
                (
                    "SELECT user_id, grafana_org_id, postgres_role, postgres_password, created_at, updated_at "
                    "FROM user_workspaces WHERE user_id = ?"
                ),
                (user_id,),
            ).fetchone()
            if row is None:
                now = time.time()
                password = secrets.token_urlsafe(32)
                connection.execute(
                    (
                        "INSERT INTO user_workspaces "
                        "(user_id, grafana_org_id, postgres_role, postgres_password, created_at, updated_at) "
                        "VALUES (?, NULL, ?, ?, ?, ?)"
                    ),
                    (user_id, role, password, now, now),
                )
                connection.commit()
                row = connection.execute(
                    (
                        "SELECT user_id, grafana_org_id, postgres_role, postgres_password, created_at, updated_at "
                        "FROM user_workspaces WHERE user_id = ?"
                    ),
                    (user_id,),
                ).fetchone()
    return dict(row)


def set_user_workspace_grafana_org(user_id: str, grafana_org_id: int) -> None:
    """Persist the Grafana org id assigned to a backend user."""
    with _DB_LOCK:
        with _connect() as connection:
            connection.execute(
                "UPDATE user_workspaces SET grafana_org_id = ?, updated_at = ? WHERE user_id = ?",
                (grafana_org_id, time.time(), user_id),
            )
            connection.commit()


def create_refresh_token(jti: str, user_id: str, exp: float) -> None:
    """Persist a refresh token.

    Args:
        jti (str): Refresh token identifier.
        user_id (str): Owning user id.
        exp (float): Expiry timestamp.

    Examples:
        >>> init_db()
        >>> name = f"doc_{uuid.uuid4().hex[:6]}"
        >>> user = create_user(name, 'hash123')
        >>> jti = f"doc_jti_{uuid.uuid4().hex[:6]}"
        >>> create_refresh_token(jti, user['id'], time.time() + 60)
        >>> get_refresh_token(jti)['jti'] == jti
        True
    """
    with _DB_LOCK:
        with _connect() as connection:
            connection.execute(
                (
                    "INSERT INTO refresh_tokens(jti, user_id, exp, revoked, created_at) "
                    "VALUES (?, ?, ?, 0, ?)"
                ),
                (jti, user_id, exp, time.time()),
            )
            connection.commit()


def get_refresh_token(jti: str) -> dict[str, Any] | None:
    """Load a refresh token row.

    Args:
        jti (str): Refresh token identifier.

    Returns:
        dict[str, Any] | None: Token row when found.

    Examples:
        >>> init_db()
        >>> token = get_refresh_token('missing-jti-example')
        >>> token is None
        True
    """
    with _connect() as connection:
        row = connection.execute(
            (
                "SELECT jti, user_id, exp, revoked, created_at "
                "FROM refresh_tokens WHERE jti = ?"
            ),
            (jti,),
        ).fetchone()
    if row is None:
        return None
    return dict(row)


def revoke_refresh_token(jti: str) -> None:
    """Mark a refresh token as revoked.

    Args:
        jti (str): Refresh token identifier.

    Examples:
        >>> init_db()
        >>> revoke_refresh_token('doc-jti')
        >>> token = get_refresh_token('doc-jti')
        >>> token is None or int(token['revoked']) == 1
        True
    """
    with _DB_LOCK:
        with _connect() as connection:
            connection.execute(
                "UPDATE refresh_tokens SET revoked = 1 WHERE jti = ?",
                (jti,),
            )
            connection.commit()
