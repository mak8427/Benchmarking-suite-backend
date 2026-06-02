#!/usr/bin/env python3
"""Reprovision Grafana resources for backend users.

This is intended for dashboard template changes that need to be applied to
existing Grafana organizations, not only users registering after the change.
"""

from __future__ import annotations

import argparse
import os
import shlex
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from api_backend.db import get_user_by_username, init_db, list_users
from api_backend.grafana import GrafanaProvisioner


def load_env_file(path: Path) -> None:
    """Load simple KEY=VALUE or fish `set -x KEY VALUE` env files."""
    if not path.exists():
        return
    for raw_line in path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        parts = shlex.split(line)
        if len(parts) >= 4 and parts[0] == "set" and parts[1] == "-x":
            os.environ.setdefault(parts[2], " ".join(parts[3:]))
        elif "=" in line:
            key, value = line.split("=", 1)
            os.environ.setdefault(key.strip(), shlex.split(value.strip())[0] if value.strip() else "")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", default=".env", help="Optional env file to load before provisioning")
    parser.add_argument("--username", help="Reprovision only one backend username")
    parser.add_argument("--dry-run", action="store_true", help="Print selected users without calling Grafana")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    load_env_file(Path(args.env_file))
    init_db()

    if args.username:
        user = get_user_by_username(args.username)
        users = [user] if user else []
    else:
        users = list_users()

    if not users:
        print("No matching users found.")
        return 1

    if args.dry_run:
        for user in users:
            print(user["username"])
        return 0

    provisioner = GrafanaProvisioner()
    if not provisioner.enabled:
        print("Grafana provisioning is disabled: missing admin credentials.")
        return 1

    failures: list[str] = []
    for user in users:
        username = user["username"]
        try:
            provisioner.provision_user(user_id=user["id"], username=username)
        except Exception as exc:  # pragma: no cover - operational reporting
            failures.append(username)
            print(f"failed {username}: {exc}")
        else:
            print(f"provisioned {username}")

    if failures:
        print(f"failed_count={len(failures)}")
        return 1
    print(f"provisioned_count={len(users)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
