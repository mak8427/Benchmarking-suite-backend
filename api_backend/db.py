"""SQLite persistence for users, refresh tokens, and jobs."""

from __future__ import annotations

import os
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
