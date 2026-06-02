#!/usr/bin/env bash
set -euo pipefail
ROOT=${BENCHMARK_BACKEND_ROOT:-$PWD}
cd "$ROOT"
export BENCHMARK_BACKEND_ROOT="$ROOT"

python3 - <<'PY'
from __future__ import annotations

import os
import re
import sqlite3
from pathlib import Path

root = Path(os.getenv('BENCHMARK_BACKEND_ROOT', os.getcwd())).resolve()
for env_path in (root / 'vars.env', root / '.env'):
    if not env_path.exists():
        continue
    for raw in env_path.read_text(errors='ignore').splitlines():
        line = raw.strip()
        if not line or line.startswith('#'):
            continue
        match = re.match(r"set\s+-x\s+(\w+)\s+(.+)$", line)
        if match:
            key, value = match.groups()
            os.environ[key] = value.strip().strip("'").strip('"')
            continue
        if line.startswith('export '):
            line = line.removeprefix('export ').strip()
        if '=' in line:
            key, value = line.split('=', 1)
            os.environ[key.strip()] = value.strip().strip("'").strip('"')

if not os.getenv('JWT_SECRET'):
    raise SystemExit('FAIL jwt secret missing from loaded backend environment')

from api_backend import main
if main.MAX_OBJECTS_PER_USER < 100_000:
    raise SystemExit(f'FAIL object quota too low: {main.MAX_OBJECTS_PER_USER}')

# The module entrypoint must run the pipeline, not import and exit.
source = (root / 'analysis_module/pipeline_runner.py').read_text()
if 'if __name__ == "__main__"' not in source or 'run_pipeline()' not in source:
    raise SystemExit('FAIL pipeline_runner has no executable module entrypoint')

con = sqlite3.connect(root / 'auth.db')
con.row_factory = sqlite3.Row
pending = con.execute('''
select count(*) from storage_objects
where original_filename like '%.h5' and state = 'presigned'
''').fetchone()[0]
print(f'pending_h5_presigned={pending}')
print('quota=' + str(main.MAX_OBJECTS_PER_USER))
print('jwt_secret_present=true')
print('pipeline_entrypoint=true')
PY

if ! pgrep -f 'uvicorn .*analysis_module.minio_listener:app' >/dev/null; then
  echo 'FAIL analysis listener is not running' >&2
  exit 1
fi

echo 'listener_running=true'
