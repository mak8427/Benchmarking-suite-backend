# Deploy the Benchmarking Suite backend from scratch

Last verified against the repository and deployment tooling: **2026-07-16**


This guide starts with a blank Ubuntu 24.04 VM. It does not copy users,
databases, dashboards, credentials, or other state from an existing backend.
The result is a new, empty deployment built from this repository.

The commands assume the Ubuntu image provides a user named `cloud` with
`sudo` access. Replace values written as `<LIKE_THIS>` before running a
command. Never commit secrets or paste them into tickets or chat.

## 1. What will be installed

The deployment uses the same shape as the reviewed production instance:

```text
Benchwrap clients
  -> public IP or DNS name
  -> K3s Traefik
       /api/*     -> FastAPI on host port 7800, with /api stripped
       /grafana/* -> Grafana on host port 3000
  -> private GWDG S3 bucket
  -> SQLite authentication and object-ownership database
  -> PostgreSQL analysis tables and per-user Grafana roles
```

The components are:

- this Git repository;
- Python 3.12 and a virtual environment;
- FastAPI/Uvicorn for authentication and S3 presigned URLs;
- SQLite for backend users, refresh tokens, and S3 ownership metadata;
- PostgreSQL 16 for processed benchmark data and row-level security;
- Grafana for per-user dashboards;
- single-node K3s, including Traefik, for public routing;
- a private GWDG S3 bucket for uploaded artifacts.

K3s is required in this deployment. Traefik runs inside K3s, and the optional
analysis listener in `analysis_module/minio_listener.py` invokes `kubectl`.
Section 12 explains why automatic webhook-created analysis Jobs are not enabled
by this clean-install procedure yet.

## 2. Requirements before starting

### VM

Use a fresh GWDG Cloud VM with:

- Ubuntu 24.04 LTS, x86-64;
- recommended: 8 vCPU, 16 GiB RAM, and 200 GB persistent disk;
- one internal OpenStack IP;
- one floating/public IP;
- outbound HTTPS access for APT, Python packages, K3s images, and DuckDB's
  PostgreSQL extension;
- an operator SSH public key installed for user `cloud`.

The recommended size matches the reviewed deployment. Smaller configurations
have not been validated by this guide.

### Network security group

Allow inbound:

- TCP `22` from administrator networks;
- TCP `80` from intended clients during initial HTTP setup;
- TCP `443` after HTTPS is configured.

Do not expose these ports publicly:

- `3000` Grafana host port;
- `5432` PostgreSQL;
- `6443` K3s API;
- `7800` raw backend API;
- `7901` optional analysis listener.

The application processes bind to all host interfaces because Traefik reaches
them through the VM's internal IP. The cloud security group is therefore an
important boundary.

### Accounts and values

Have these ready:

- read access to this GitLab repository;
- a private GWDG S3 bucket;
- an S3 access key allowed to list the bucket and read/write objects;
- a new JWT secret;
- a new PostgreSQL application password;
- a new Grafana administrator password;
- the VM's public and internal IP addresses;
- optionally, a DNS name for HTTPS.

The backend does not create the S3 bucket. Create it first in GWDG Cloud and
keep public access disabled.

## 3. Connect and prepare Ubuntu

From the operator's workstation:

```bash
ssh cloud@<PUBLIC_IP>
```

On the VM:

```bash
sudo apt update
sudo apt full-upgrade -y
sudo apt install -y \
  git \
  curl \
  ca-certificates \
  gnupg \
  openssl \
  python3 \
  python3-venv \
  python3-pip \
  postgresql \
  postgresql-client \
  sqlite3
sudo timedatectl set-timezone Europe/Berlin
sudo systemctl enable --now postgresql
```

Verify the starting point:

```bash
hostnamectl
python3 --version
git --version
psql --version
sudo systemctl is-active postgresql
free -h
df -hT /
```

On Ubuntu 24.04, the expected PostgreSQL major version is 16. Confirm with:

```bash
pg_lsclusters
```

If `/var/run/reboot-required` exists after the upgrade, reboot and reconnect
before installing K3s:

```bash
if test -f /var/run/reboot-required; then
  echo 'Reboot required before continuing'
fi
```

## 4. Install and configure K3s

Create a group that will later permit the `cloud` service account to use the
K3s kubeconfig without making it world-readable:

```bash
sudo groupadd --system k3s-admin
sudo usermod -aG k3s-admin cloud
sudo install -d -m 0755 /etc/rancher/k3s
sudo tee /etc/rancher/k3s/config.yaml >/dev/null <<'YAML'
write-kubeconfig-mode: "0640"
write-kubeconfig-group: k3s-admin
YAML
```

Install the version verified on the existing architecture:

```bash
curl -sfL https://get.k3s.io | INSTALL_K3S_VERSION='v1.33.6+k3s1' sh -
```

Log out and reconnect so the new group membership applies, then verify:

```bash
exit
ssh cloud@<PUBLIC_IP>
export KUBECONFIG=/etc/rancher/k3s/k3s.yaml
kubectl get nodes -o wide
kubectl get pods -A
```

Wait until CoreDNS, Traefik, the local-path provisioner, and metrics server are
Running or Completed as appropriate.

## 5. Clone and test the repository

Clone with a recipient-owned GitLab credential or deploy key:

```bash
cd /home/cloud
git clone <GITLAB_REPOSITORY_URL> Benchmarking-suite-backend
cd Benchmarking-suite-backend
git status --short --branch
git log -1 --oneline
```

Create an isolated Python environment and install runtime dependencies:

```bash
python3 -m venv .venv
.venv/bin/python -m pip install --upgrade pip wheel
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python -m pip check
```

`argon2-cffi` is a required runtime dependency because registration calls
`passlib.hash.argon2.hash()`. It must be present even though Passlib itself can
be installed without that optional backend.

Install the test runner and run the repository tests:

```bash
.venv/bin/python -m pip install pytest
.venv/bin/python -m pytest api_backend/tests -ra
```

Do not proceed if authentication, storage, or normalized-dashboard tests fail.

## 6. Create a dedicated PostgreSQL database and role

The backend role needs `CREATEROLE` because registration provisions a separate
read-only PostgreSQL role for every Grafana user.

Open PostgreSQL interactively:

```bash
sudo -u postgres psql
```

Run the following SQL, substituting a strong password. Using an interactive
session avoids leaving the password in the shell command history.

```sql
CREATE ROLE bench_admin WITH LOGIN CREATEROLE PASSWORD '<POSTGRES_PASSWORD>';
CREATE DATABASE benchmarking_suite OWNER bench_admin;
\connect benchmarking_suite
GRANT ALL ON SCHEMA public TO bench_admin;
\quit
```

PostgreSQL should remain reachable only on loopback:

```bash
sudo -u postgres psql -Atqc 'show listen_addresses'
sudo ss -lntp | grep 5432
```

Test password authentication:

```bash
psql 'host=127.0.0.1 port=5432 dbname=benchmarking_suite user=bench_admin' \
  -c 'select current_user, current_database();'
```

## 7. Install and configure Grafana

Install Grafana OSS from its signed APT repository:

```bash
sudo install -d -m 0755 /etc/apt/keyrings
sudo curl --fail --silent --show-error --location \
  --output /etc/apt/keyrings/grafana.asc \
  https://apt.grafana.com/gpg-full.key
sudo chmod 644 /etc/apt/keyrings/grafana.asc
echo 'deb [signed-by=/etc/apt/keyrings/grafana.asc] https://apt.grafana.com stable main' \
  | sudo tee /etc/apt/sources.list.d/grafana.list
sudo apt update
sudo apt install -y grafana
```

Edit `/etc/grafana/grafana.ini` and set:

```ini
[server]
domain = <PUBLIC_IP_OR_DOMAIN>
root_url = %(protocol)s://%(domain)s/grafana/
serve_from_sub_path = true

[users]
allow_sign_up = false

[auth.proxy]
enabled = false
```

The backend currently provisions a Grafana user with the same username and
password supplied to `/auth/register`; it does not use Grafana auth proxy in
the verified configuration.

Start Grafana:

```bash
sudo systemctl enable --now grafana-server
curl -fsS http://127.0.0.1:3000/api/health
```

Set a new Grafana administrator password. Avoid placing the literal password
in a saved script:

```bash
read -rsp 'New Grafana admin password: ' GRAFANA_ADMIN_PASSWORD
echo
sudo -v
printf '%s\n' "$GRAFANA_ADMIN_PASSWORD" | sudo /usr/share/grafana/bin/grafana cli \
  --homepath /usr/share/grafana \
  --config /etc/grafana/grafana.ini \
  admin reset-admin-password --password-from-stdin
unset GRAFANA_ADMIN_PASSWORD
```

The default administrator username is normally `admin`. Use that username in
the backend environment unless it was changed.

## 8. Create backend state directories and configuration

Create dedicated writable locations instead of placing mutable state in Git:

```bash
sudo install -d -o cloud -g cloud -m 0700 /var/lib/benchmark-suite
sudo install -d -o cloud -g cloud -m 0750 /var/log/benchmark-suite
sudo install -d -o root -g cloud -m 0750 /etc/benchmark-suite
```

Generate a JWT secret:

```bash
openssl rand -hex 32
```

Create `/etc/benchmark-suite/backend.env` with real values:

```dotenv
JWT_SECRET=<OUTPUT_FROM_OPENSSL>
STRICT_CONFIG=1

S3_ENDPOINT_URL=https://s3.gwdg.de
S3_ACCESS_KEY_ID=<S3_ACCESS_KEY>
S3_SECRET_ACCESS_KEY=<S3_SECRET_KEY>
S3_BUCKET=<S3_BUCKET_NAME>
S3_ADDRESSING_STYLE=path
S3_REGION=us-east-1
AWS_EC2_METADATA_DISABLED=true

POSTGRES_HOST=127.0.0.1
POSTGRES_PORT=5432
POSTGRES_DB=benchmarking_suite
POSTGRES_USER=bench_admin
POSTGRES_PASSWORD=<POSTGRES_PASSWORD>

GRAFANA_URL=http://127.0.0.1:3000
GRAFANA_ADMIN_USER=admin
GRAFANA_ADMIN_PASSWORD=<GRAFANA_ADMIN_PASSWORD>

AUTH_DB_PATH=/var/lib/benchmark-suite/auth.db
LOG_FILE_PATH=/var/log/benchmark-suite/api.log
MAX_OBJECTS_PER_USER=100000
MAX_STORAGE_BYTES_PER_USER=10737418240
```

If a value contains whitespace, `#`, or shell metacharacters, quote it using
systemd EnvironmentFile syntax. Secure the file:

```bash
sudo chown root:cloud /etc/benchmark-suite/backend.env
sudo chmod 640 /etc/benchmark-suite/backend.env
```

Do not create a repository `.env` containing production secrets.

## 9. Initialize the empty SQLite and PostgreSQL schemas

The API startup hook creates the SQLite tables automatically. Initialize them
explicitly now so permissions and location can be checked before systemd starts:

```bash
sudo -u cloud bash -lc '
  set -a
  source /etc/benchmark-suite/backend.env
  set +a
  cd /home/cloud/Benchmarking-suite-backend
  .venv/bin/python -c "from api_backend.db import init_db; init_db()"
'
sudo chmod 600 /var/lib/benchmark-suite/auth.db
```

Initialize the normalized PostgreSQL tables and RLS policies:

```bash
sudo -u cloud bash -lc '
  set -a
  source /etc/benchmark-suite/backend.env
  set +a
  cd /home/cloud/Benchmarking-suite-backend
  .venv/bin/python -c "from analysis_module.connectors.normalized import prepare_postgres_normalized_schema; prepare_postgres_normalized_schema()"
'
```

Verify both stores:

```bash
stat -c '%a %U:%G %n' /var/lib/benchmark-suite/auth.db
sudo -u postgres psql -d benchmarking_suite -c '\dt public.*'
sudo -u postgres psql -d benchmarking_suite -c \
  "select tablename, rowsecurity from pg_tables where schemaname='public' order by tablename;"
```

The expected normalized tables are:

- `benchmark_jobs`;
- `benchmark_samples`;
- `benchmark_likwid_samples`.

## 10. Install the backend API as a systemd service

Create `/etc/systemd/system/benchmark-api.service`:

```ini
[Unit]
Description=Benchmarking Suite FastAPI backend
Wants=network-online.target postgresql.service grafana-server.service
After=network-online.target postgresql.service grafana-server.service

[Service]
Type=simple
User=cloud
Group=cloud
WorkingDirectory=/home/cloud/Benchmarking-suite-backend
EnvironmentFile=/etc/benchmark-suite/backend.env
Environment=PYTHONUNBUFFERED=1
ExecStart=/home/cloud/Benchmarking-suite-backend/.venv/bin/python -m uvicorn main:app --host 0.0.0.0 --port 7800
Restart=on-failure
RestartSec=5
UMask=0077
NoNewPrivileges=true
PrivateTmp=true

[Install]
WantedBy=multi-user.target
```

Enable and verify it:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now benchmark-api
systemctl status benchmark-api --no-pager
curl -fsS http://127.0.0.1:7800/healthz
sudo journalctl -u benchmark-api -n 100 --no-pager
```

Expected response:

```json
{"status":"ok"}
```

The health endpoint only proves that Uvicorn is serving requests. It does not
test S3, PostgreSQL, or Grafana; those are tested separately below.

## 11. Route `/api` and `/grafana` through K3s Traefik

Find the VM's internal OpenStack IPv4 address:

```bash
ip -4 address show
```

Create `/home/cloud/benchmarking-suite-routing.yaml`, replacing
`<INTERNAL_VM_IP>`:

```yaml
apiVersion: v1
kind: Service
metadata:
  name: bench-api-host
  namespace: default
spec:
  ports:
    - name: http
      port: 7800
      targetPort: 7800
---
apiVersion: discovery.k8s.io/v1
kind: EndpointSlice
metadata:
  name: bench-api-host
  namespace: default
  labels:
    kubernetes.io/service-name: bench-api-host
addressType: IPv4
ports:
  - name: http
    protocol: TCP
    port: 7800
endpoints:
  - addresses:
      - <INTERNAL_VM_IP>
---
apiVersion: v1
kind: Service
metadata:
  name: grafana-host
  namespace: default
spec:
  ports:
    - name: http
      port: 3000
      targetPort: 3000
---
apiVersion: discovery.k8s.io/v1
kind: EndpointSlice
metadata:
  name: grafana-host
  namespace: default
  labels:
    kubernetes.io/service-name: grafana-host
addressType: IPv4
ports:
  - name: http
    protocol: TCP
    port: 3000
endpoints:
  - addresses:
      - <INTERNAL_VM_IP>
---
apiVersion: traefik.io/v1alpha1
kind: Middleware
metadata:
  name: strip-api-prefix
  namespace: default
spec:
  stripPrefix:
    prefixes:
      - /api
---
apiVersion: traefik.io/v1alpha1
kind: IngressRoute
metadata:
  name: benchmarking-suite-web
  namespace: default
spec:
  entryPoints:
    - web
  routes:
    - kind: Rule
      match: PathPrefix(`/api`)
      middlewares:
        - name: strip-api-prefix
      services:
        - name: bench-api-host
          port: 7800
    - kind: Rule
      match: PathPrefix(`/grafana`)
      services:
        - name: grafana-host
          port: 3000
```

Apply and test:

```bash
export KUBECONFIG=/etc/rancher/k3s/k3s.yaml
kubectl apply -f /home/cloud/benchmarking-suite-routing.yaml
kubectl get services,endpointslices,ingressroutes,middlewares -n default
curl -fsS http://<PUBLIC_IP>/api/healthz
curl -fsS http://<PUBLIC_IP>/grafana/api/health
```

This establishes HTTP parity. Before production use, assign a DNS name and add
a valid TLS certificate through Traefik. Merely creating a `websecure` route
does not create a trusted certificate. After HTTPS is working, change Grafana's
`domain` and `root_url`, restart Grafana, and configure clients with:

```bash
export BENCHWRAP_API_URL=https://<DOMAIN>/api
```

## 12. Analysis processing and the Kubernetes limitation

### Supported clean-install path: run analysis on the host

After a user uploads HDF5 objects, process their S3 prefix manually. The prefix
must be the backend user ID, not the username:

```bash
sudo -u cloud bash -lc '
  set -a
  source /etc/benchmark-suite/backend.env
  export S3_SYNC=1
  export S3_OBJECT_PREFIX=<BACKEND_USER_ID>
  set +a
  cd /home/cloud/Benchmarking-suite-backend
  .venv/bin/python -m analysis_module.pipeline_runner --allow-missing-source
'
```

The first pipeline run downloads DuckDB's PostgreSQL extension, reads matching
S3 objects, creates raw per-file tables, and upserts the normalized dashboard
tables.

### Why the webhook listener is not enabled by this guide

The code intends `analysis_module/minio_listener.py` to execute:

```text
kubectl create job ... --image=localhost/duckdb-analysis:latest
```

However, a blank clone does not yet provide a complete production Job setup:

1. No analysis image is published to a registry by this repository.
2. `analysis_module/Dockerfile` has an outdated default module path, although
   the listener overrides the container command.
3. The dynamically created Job receives `S3_SYNC`, `S3_BUCKET`, and an object
   prefix, but it does not receive S3 credentials or PostgreSQL settings.
4. `POSTGRES_HOST=127.0.0.1` would refer to the Job container, not the host
   PostgreSQL process.
5. The Job does not mount `auth.db`, so it cannot reliably recover the uploaded
   object's backend ownership metadata.
6. The listener needs Kubernetes permission to create Jobs. Giving the host
   process the K3s administrator kubeconfig would be broader than necessary.
7. The repository has no ready-to-apply ServiceAccount, Role, RoleBinding,
   Secret, or Job template that resolves these points.

Therefore, K3s is installed and used for Traefik, but automatic webhook Jobs
must not be described as working from the repository alone. Before enabling
the listener, add and test:

- a corrected, versioned analysis image in an accessible registry;
- a Kubernetes Secret for S3/PostgreSQL settings;
- a reachable PostgreSQL service/address;
- a safe ownership-metadata mechanism;
- a least-privilege ServiceAccount and Role limited to analysis Jobs;
- a maintained Job manifest or controller instead of an underspecified
  imperative `kubectl create job` command;
- authenticated webhook exposure and an S3 notification configuration.

## 13. End-to-end validation

### Service state

```bash
systemctl is-enabled benchmark-api grafana-server postgresql k3s
systemctl is-active benchmark-api grafana-server postgresql k3s
curl -fsS http://127.0.0.1:7800/healthz
curl -fsS http://127.0.0.1:3000/api/health
curl -fsS http://<PUBLIC_IP>/api/healthz
curl -fsS http://<PUBLIC_IP>/grafana/api/health
kubectl get nodes,pods -A
```

### S3 credentials and bucket

```bash
sudo -u cloud bash -lc '
  set -a
  source /etc/benchmark-suite/backend.env
  set +a
  cd /home/cloud/Benchmarking-suite-backend
  .venv/bin/python - <<"PY"
from api_backend.storage.minio_client import ADMIN_MINIO, BUCKET

names = [bucket.name for bucket in ADMIN_MINIO.list_buckets()]
assert BUCKET in names, f"configured bucket {BUCKET!r} is not visible"
print(f"S3 OK: {BUCKET}")
PY
'
```

If the S3 account can access a bucket but cannot list all buckets, replace this
check with a prefix listing against the configured bucket.

### Registration and Grafana provisioning

Registering a user creates persistent state. Use an intended initial account or
a clearly named disposable account:

```bash
curl --fail-with-body -X POST \
  'http://<PUBLIC_IP>/api/auth/register' \
  -H 'Content-Type: application/json' \
  -d '{"username":"deploymentcheck","password":"<TEST_PASSWORD>"}'
```

Important: the registration route catches Grafana provisioning errors and can
still return HTTP 201. A successful response is not enough. Immediately check:

```bash
sudo journalctl -u benchmark-api -n 100 --no-pager
sudo -u postgres psql -d benchmarking_suite -c \
  "select rolname from pg_roles where rolname like 'bench_user_%';"
```

Then log into `http://<PUBLIC_IP>/grafana/` with the same test username and
password. Confirm the private organization, PostgreSQL datasource, and starter
dashboard exist.

### Reboot test

```bash
sudo reboot
```

After reconnecting, repeat all service health checks. The deployment is not
complete until it survives a reboot without a manual Uvicorn or Grafana start.

## 14. Routine updates and backups

Update application code:

```bash
cd /home/cloud/Benchmarking-suite-backend
git fetch origin
git log --oneline HEAD..origin/master
git pull --ff-only origin master
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python -m pip check
sudo systemctl restart benchmark-api
curl -fsS http://127.0.0.1:7800/healthz
```

Inspect logs:

```bash
sudo journalctl -u benchmark-api -f
sudo journalctl -u grafana-server -f
sudo journalctl -u postgresql -f
sudo journalctl -u k3s -f
```

Back up at minimum:

- `/var/lib/benchmark-suite/auth.db` using SQLite's backup API;
- the `benchmarking_suite` PostgreSQL database;
- PostgreSQL global roles, because Grafana uses per-user roles;
- `/var/lib/grafana/grafana.db` and `/etc/grafana`;
- `/etc/benchmark-suite/backend.env` in an approved encrypted secret store;
- `/home/cloud/benchmarking-suite-routing.yaml`;
- the deployed Git commit ID.

Keep the S3 bucket private and configure its retention/versioning policy in
GWDG Cloud separately.

## 15. Code-derived behavior to remember

- API startup calls `api_backend.db.init_db()` and creates SQLite tables.
- `/healthz` returns a static status and is not a deep dependency check.
- `STRICT_CONFIG=1` makes missing JWT/S3 settings stop API startup.
- `/auth/register` stores the backend user before attempting Grafana setup.
- Grafana provisioning failures are logged but do not roll back registration.
- Grafana provisioning creates a private org, user, PostgreSQL datasource,
  dashboard, and a PostgreSQL role scoped with RLS. That role receives `SELECT`
  on all three normalized dashboard tables, including LIKWID samples.
- Normalized PostgreSQL tables are created by
  `prepare_postgres_normalized_schema()` or the analysis pipeline, not by API
  startup.
- S3 upload URLs use private ACLs and keys beneath the backend user ID prefix.
- The repository's Python dependencies install the runtime, but `pytest` is a
  separate test/development dependency.

## 16. Official installation references

- [K3s quick-start](https://docs.k3s.io/quick-start)
- [K3s server options](https://docs.k3s.io/cli/server)
- [K3s image importing](https://docs.k3s.io/add-ons/import-images)
- [Grafana on Debian or Ubuntu](https://grafana.com/docs/grafana/latest/setup-grafana/installation/debian/)
- [PostgreSQL packages for Ubuntu](https://www.postgresql.org/download/linux/ubuntu/)
