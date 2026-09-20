# Runbook: DuckDB `MERGE INTO` on Iceberg tables — fully local edition

This is a step-by-step, locally reproducible version of the exercise in
**["DuckDB Now Supports MERGE on Iceberg Tables"](https://medium.com/@shahsoumil519/duckdb-now-supports-merge-on-iceberg-tables-4e4363b925c4)**
(Soumil Shah, June 2026), which uses **AWS S3 Tables / Glue**. Here everything
runs on one machine with open-source equivalents:

| Article (AWS) | This runbook (local) | Container / endpoint |
| --- | --- | --- |
| Amazon S3 (data + metadata files) | **RustFS** (S3-compatible object storage) | `rustfs/rustfs:1.0.0` — API `http://localhost:9000`, console `http://localhost:9001` |
| Glue / S3 Tables catalog (catalog metadata) | **PostgreSQL** holding the catalog, exposed through **Lakekeeper**, an Iceberg **REST catalog** | `postgres:17` on `localhost:5432` + `quay.io/lakekeeper/catalog:v0.13.5` on `http://localhost:8181/catalog` |
| `ATTACH ... ENDPOINT_TYPE s3_tables` | `ATTACH 'demo' AS lake (TYPE iceberg, ENDPOINT 'http://localhost:8181/catalog')` | DuckDB iceberg extension |
| `CREATE TABLE s3_tables_db.lab1.customers2 (...)`, `MERGE INTO ...` | Identical SQL, unchanged | DuckDB **≥ 1.5.3** (MERGE INTO on Iceberg landed in 1.5.3) |

This project contains **two exercises** on the same local stack:

| Exercise | Article | Runbook | Entry point |
| --- | --- | --- | --- |
| 1. MERGE on Iceberg tables | [DuckDB Now Supports MERGE on Iceberg Tables](https://medium.com/@shahsoumil519/duckdb-now-supports-merge-on-iceberg-tables-4e4363b925c4) | this file | `python python/run_merge_exercise.py` |
| 2. Iceberg **v3** + **VARIANT** reads | [Can DuckDB Read Iceberg V3 VARIANT from Amazon S3 Tables? Quick Test](https://medium.com/@shahsoumil519/can-duckdb-read-iceberg-v3-variant-from-amazon-s3-tables-quick-test-a191ab70bf80) | `README-v3-variant.md` | `python python/run_v3_variant_exercise.py` |

Both exercises share one warehouse (`demo`) and one RustFS bucket; they use different
namespaces (`lab1` for exercise 1, `test` for exercise 2), so they can coexist and be
re-run in any order.

Everything in this document was executed end-to-end on the machine that
generated this repo; the outputs shown are real (see “Verified setup” at the
bottom for exact versions).

```
                       host machine (Windows / macOS / Linux)
  ┌────────────────────────────────────────────────────────────────────────┐
  │  DuckDB (python)                                                       │
  │   1. CREATE SECRET ... TYPE s3        → talks S3 directly              │
  │   2. CREATE SECRET ... TYPE iceberg   → bearer token for the catalog    │
  │   3. ATTACH 'demo' AS lake (TYPE iceberg, ENDPOINT :8181/catalog)      │
  └───────────────▲──────────────────────────────┬─────────────────────────┘
                  │ REST (Iceberg REST spec)     │ S3 API (path style, http)
                  │ http://localhost:8181/catalog│ http://host.docker.internal:9000
  ┌───────────────┴──────────────┐   ┌───────────▼─────────────────────────┐
  │ container: lakekeeper v0.13.5│   │ container: rustfs 1.0.0             │
  │  - Iceberg REST catalog      │   │  - bucket: warehouse                │
  │  - vends STS creds to DuckDB │   │  - parquet data files               │
  └───────────────┬──────────────┘   │  - metadata/*.metadata.json         │
                  │ SQL              │  - metadata/*.avro (manifests, snaps)│
  ┌───────────────▼──────────────┐   └───────────▲─────────────────────────┘
  │ container: postgres 17       │               │
  │  - db: iceberg               │               │ object put/get
  │  - tables: warehouse,        │───────────────┘
  │    namespace, tabular,       │  (Lakekeeper reads/writes storage too)
  │    table, table_snapshot ... │
  └──────────────────────────────┘
```

### Repository layout

```
docker-compose.yml               # RustFS + PostgreSQL + Lakekeeper + one-shot init jobs
config/create-warehouse.json     # Lakekeeper warehouse pointing at the RustFS bucket
sql/01_secrets_and_attach.sql    # extensions, S3 secret, catalog token, ATTACH
sql/02_create_table_and_seed.sql # CREATE SCHEMA/TABLE + seed rows
sql/03_merge_upsert.sql          # MERGE: UPDATE matched + INSERT not matched
sql/04_merge_delete.sql          # MERGE: DELETE matched
sql/05_merge_insert_only.sql     # MERGE: INSERT not matched only
sql/06_verify_snapshots.sql      # one Iceberg snapshot per write
sql/07_verify_files.sql          # parquet data + positional-delete files
python/run_merge_exercise.py     # runs all of the above in order (single entry point)
```
Exercise 2 (Iceberg v3 + VARIANT, see `README-v3-variant.md`) adds:

```
sql-v3-variant/                  # 17 SQL files: the 4 article tests + v3 extras
data/sample_github_events.ndjson # GH-Archive-shaped sample for the file-ingestion step
python/run_v3_variant_exercise.py# runs sql-v3-variant/*.sql in order
```




---

## Step 0 — Prerequisites

| Requirement | Notes |
| --- | --- |
| Docker Desktop (or Docker Engine + Compose v2) **running** | `docker info` must succeed. On Windows use Linux containers. |
| Python ≥ 3.9 | `python --version` (validated with 3.14.7) |
| `duckdb` Python package **≥ 1.5.3** | `MERGE INTO` against Iceberg tables was added in DuckDB v1.5.3 |
| Free TCP ports | `5432` (Postgres), `8181` (Lakekeeper), `9000` + `9001` (RustFS) |

```bash
python -m pip install "duckdb>=1.5.3"
python -c "import duckdb; print(duckdb.__version__)"   # -> 1.5.5 (or newer)
```

> Windows tip: `docker compose ...` prints progress on stderr, which Windows
> PowerShell surfaces as scary `NativeCommandError` noise. Either ignore it or
> wrap commands as `cmd /c "docker compose up -d"`. It is not a failure.

---

## Step 1 — Start the local lakehouse

```bash
docker compose up -d
```

First run pulls 5 images (~450 MB) and then, automatically:

1. `rustfs-init` — creates the S3 bucket `warehouse` in RustFS.
2. `lakekeeper-migrate` — creates the catalog tables inside PostgreSQL (`iceberg` db).
3. `lakekeeper-bootstrap` — accepts the Lakekeeper terms of use (HTTP 204).
4. `lakekeeper-warehouse-init` — registers warehouse **`demo`** backed by the RustFS bucket (HTTP 201).

Check the result:

```bash
docker compose ps
```

Expected (the one-shot jobs end as `Exited (0)`, which is success):

```
NAME                       STATUS
iceberg-postgres           Up (healthy)
lakekeeper                 Up (healthy)
rustfs                     Up
rustfs-init                Exited (0)
lakekeeper-migrate         Exited (0)
lakekeeper-bootstrap       Exited (0)
lakekeeper-warehouse-init  Exited (0)
```

If any `*-init` job is not `Exited (0)`, read its log — the troubleshooting
table at the bottom covers every failure mode seen while building this.

```bash
docker compose logs rustfs-init lakekeeper-bootstrap lakekeeper-warehouse-init
# rustfs-init               | Bucket created successfully `rustfs/warehouse`.
# lakekeeper-bootstrap      | bootstrap -> HTTP 204
# lakekeeper-warehouse-init | warehouse create -> HTTP 201
```

### Step 2 — Sanity-check the three services by hand (optional but useful)

```bash
# RustFS S3 API
curl -i http://localhost:9000/health                     # -> HTTP 200

# Lakekeeper: unsecured local catalog, so any bearer token is accepted.
# NOTE the /catalog base path — http://localhost:8181/v1/config returns 404.
curl -s -H "Authorization: Bearer dummy" \
  "http://localhost:8181/catalog/v1/config?warehouse=demo"

# Warehouse registration (what the catalog knows about storage)
curl -s "http://localhost:8181/management/v1/warehouse?name=demo"
```

The `config` call returns the REST capabilities plus the catalog `prefix`:

```json
{"overrides":{"uri":"http://localhost:8181/catalog","idempotency-key-lifetime":"PT30M"},
 "defaults":{"rest-page-size":"100","prefix":"<warehouse-uuid>"},
 "endpoints":["GET /v1/config","GET /v1/{prefix}/namespaces","..."]}
```

RustFS also has a web console: <http://localhost:9001> (log in with
`lakehouse-admin` / `lakehouse-admin-secret`) — buckets and objects are visible
there, which is handy for the “my files really are in object storage” check.

---

## Step 3 — Understand the four SQL pieces

### 3a. Secrets + `ATTACH` (`sql/01_secrets_and_attach.sql`)

```sql
INSTALL httpfs; LOAD httpfs;
INSTALL iceberg; LOAD iceberg;

CREATE OR REPLACE SECRET rustfs_s3 (          -- where the bytes live
    TYPE s3,
    KEY_ID 'lakehouse-admin',
    SECRET 'lakehouse-admin-secret',
    REGION 'us-east-1',
    ENDPOINT 'host.docker.internal:9000',     -- RustFS S3 API, no scheme
    URL_STYLE 'path',                         -- RustFS/MinIO style addressing
    USE_SSL false                             -- local, no TLS
);

CREATE OR REPLACE SECRET lakekeeper_catalog ( -- how to talk to the catalog
    TYPE iceberg,
    TOKEN 'dummy'                             -- unsecured local catalog: any token
);

ATTACH 'demo' AS lake (                       -- 'demo' = warehouse name
    TYPE iceberg,
    ENDPOINT 'http://localhost:8181/catalog', -- Lakekeeper REST base path
    SECRET lakekeeper_catalog
);
```

Two details that cost the most time while building this, so they are called out
explicitly:

* the REST **base path** is `/catalog`, not `/`;
* the S3 endpoint in the **warehouse profile** is used by *both* Lakekeeper
  (in a container) and DuckDB (on your host), so it must resolve in both places
  — that is why it is `host.docker.internal:9000` and not `rustfs:9000`
  (container-only) or `localhost:9000` (host-only).

The `rustfs_s3` secret is *optional* once the warehouse vends STS credentials
(see 3b); it is kept because it also lets you read raw paths such as
`iceberg_scan('s3://warehouse/...')` without a catalog, and it keeps the setup
working if you later turn credential vending off.

### 3b. Why the warehouse must vend credentials

`config/create-warehouse.json` registers the warehouse with Lakekeeper:

```json
{
  "warehouse-name": "demo",
  "project-id": "00000000-0000-0000-0000-000000000000",
  "storage-profile": {
    "type": "s3",
    "bucket": "warehouse",
    "key-prefix": "iceberg",
    "endpoint": "http://host.docker.internal:9000",
    "sts-endpoint": "http://rustfs:9000",
    "sts-role-arn": "arn:aws:iam::000000000000:role/lakekeeper",
    "region": "us-east-1",
    "path-style-access": true,
    "flavor": "s3-compat",
    "sts-enabled": true
  },
  "storage-credential": {
    "type": "s3",
    "credential-type": "access-key",
    "access-key-id": "lakehouse-admin",
    "secret-access-key": "lakehouse-admin-secret"
  }
}
```

When DuckDB loads a table, Lakekeeper answers
`GET /v1/{prefix}/namespaces/{ns}/tables/{t}/credentials` with either

* **`sts-enabled: true`** → temporary RustFS STS credentials
  (`s3.access-key-id`, `s3.secret-access-key`, `s3.session-token`, expiry), so
  DuckDB reads/writes with no S3 keys of its own, or
* **`sts-enabled: false`** → only `s3.endpoint` / `s3.path-style-access`
  (no keys); DuckDB then sends an unsigned request and RustFS replies
  `403 AccessDenied`.

RustFS implements `AssumeRole`, which is what makes vending work here (you can
check it independently: boto3/`aws sts assume-role` against
`http://localhost:9000` with the same keys returns a session token).

### 3c. The three `MERGE` shapes (identical to the article)

```sql
-- Upsert: update matched rows, insert new ones
MERGE INTO lake.lab1.customers2 AS target
USING (FROM (VALUES (1,'Alice','Boston',150.00), (2,'Bob','Portland',250.50), (4,'Dan','Denver',300.00))
       t(customer_id, name, city, balance)) AS upserts
ON target.customer_id = upserts.customer_id
WHEN MATCHED THEN UPDATE
WHEN NOT MATCHED THEN INSERT;

-- Delete a set of rows
MERGE INTO lake.lab1.customers2 AS target
USING (FROM (VALUES (3)) t(customer_id)) AS deletes
ON target.customer_id = deletes.customer_id
WHEN MATCHED THEN DELETE;

-- Insert-only
MERGE INTO lake.lab1.customers2 AS target
USING (FROM (VALUES (5,'Eve','Miami',50.00), (6,'Frank','Chicago',125.00))
       t(customer_id, name, city, balance)) AS new_rows
ON target.customer_id = new_rows.customer_id
WHEN NOT MATCHED THEN INSERT;
```

Iceberg tables have no primary key, so `MERGE INTO` is the recommended upsert
primitive. DuckDB implements it with **merge-on-read** semantics: matched rows
are not rewritten, they are marked with positional-delete files (Step 6).
`MERGE INTO` on Iceberg requires **DuckDB ≥ 1.5.3** and is limited to
merge-on-read (copy-on-write is not supported yet), which is why the table must
not carry `write.update.mode` / `write.delete.mode` properties other than
`merge-on-read`.

---

## Step 4 — Run the whole exercise

```bash
python python/run_merge_exercise.py
```

Real output (DuckDB 1.5.5, 2026-09-20):

```
Extensions in use
  extension_name | extension_version
  ---------------+------------------
  httpfs         | 827222f
  iceberg        | 45163a28
  (2 rows)
duckdb (pip) version: 1.5.5
catalog endpoint: http://localhost:8181/catalog
S3 endpoint:      host.docker.internal:9000

=== 01_secrets_and_attach.sql ===================================
  database | schema | name | column_names | column_types | temporary
  ---------+--------+------+--------------+--------------+----------
  (0 rows)                                   <- catalog attached, no tables yet

=== 02_create_table_and_seed.sql ================================
  01-load | 1 | Alice | Boston  | 100.0
  01-load | 2 | Bob   | Seattle | 200.0
  01-load | 3 | Carol | Austin  | 300.0
  (3 rows)

=== 03_merge_upsert.sql =========================================
  02-upsert | 1 | Alice | Boston   | 150.0    <- updated
  02-upsert | 2 | Bob   | Portland | 250.5    <- updated
  02-upsert | 3 | Carol | Austin   | 300.0    <- untouched
  02-upsert | 4 | Dan   | Denver   | 300.0    <- inserted
  (4 rows)

=== 04_merge_delete.sql =========================================
  03-delete | 1 | Alice | Boston   | 150.0
  03-delete | 2 | Bob   | Portland | 250.5
  03-delete | 4 | Dan   | Denver   | 300.0    <- Carol (3) deleted
  (3 rows)

=== 05_merge_insert_only.sql ====================================
  04-insert-only | 1 | Alice | Boston   | 150.0
  04-insert-only | 2 | Bob   | Portland | 250.5
  04-insert-only | 4 | Dan   | Denver   | 300.0
  04-insert-only | 5 | Eve   | Miami    | 50.0     <- inserted
  04-insert-only | 6 | Frank | Chicago  | 125.0    <- inserted
  (5 rows)

=== 06_verify_snapshots.sql =====================================
  snapshot_id         | sequence_number | timestamp_ms               | manifest_list
  --------------------+-----------------+----------------------------+---------------------------------------------
  4939991677750035458 | 1 | 2026-09-20 20:20:48.673000 | s3://warehouse/iceberg/<table-uuid>/metadata/snap-*.avro
  8480792010504618233 | 2 | 2026-09-20 20:20:48.753000 | s3://warehouse/iceberg/<table-uuid>/metadata/snap-*.avro
  8833976983826878189 | 3 | 2026-09-20 20:20:48.790000 | s3://warehouse/iceberg/<table-uuid>/metadata/snap-*.avro
  5644086141066627665 | 4 | 2026-09-20 20:20:48.873000 | s3://warehouse/iceberg/<table-uuid>/metadata/snap-*.avro
  1471020164788667471 | 5 | 2026-09-20 20:20:48.961000 | s3://warehouse/iceberg/<table-uuid>/metadata/snap-*.avro
  (5 rows)

=== 07_verify_files.sql =========================================
  manifest_content | content          | file_format | record_count
  -----------------+------------------+-------------+-------------
  DATA             | EXISTING         | parquet     | 3
  DATA             | EXISTING         | parquet     | 2
  DATA             | EXISTING         | parquet     | 1
  DATA             | EXISTING         | parquet     | 2
  DELETE           | POSITION_DELETES | parquet     | 2
  DELETE           | POSITION_DELETES | parquet     | 1
  (6 rows)

Done: all MERGE statements committed as Iceberg snapshots.
```

Reading the numbers: five snapshots = initial insert + one commit per write
statement; the two `POSITION_DELETES` files are the merge-on-read deletes
produced by the update/delete merges. Final table content is
`{1 Alice, 2 Bob, 4 Dan, 5 Eve, 6 Frank}`.

The runner executes `sql/*.sql` in order, substitutes placeholders from the
environment (`RUSTFS_ACCESS_KEY`, `RUSTFS_SECRET_KEY`, `S3_REGION`,
`S3_ENDPOINT`, `CATALOG_ENDPOINT`, `ICEBERG_TOKEN` — defaults are the local
ones) and prints the last result set of each file. Re-running is safe: the seed
file deletes rows first, so the end state is always the same.

Prefer the DuckDB CLI? `duckdb :memory: -f sql/01_secrets_and_attach.sql` then
the other files works too, after replacing the `${...}` placeholders with real
values (or using `envsubst` on Linux/macOS). The Python runner is the path
validated on Windows.

---

## Step 5 — Prove the catalog metadata is in PostgreSQL

Lakekeeper stores all catalog state in the `iceberg` database (schema
`public`): `warehouse`, `namespace`, `tabular`, `table`, `table_snapshot`,
`table_metadata_log`, … The table row and its snapshot count:

```bash
docker compose exec -T postgres psql -U postgres -d iceberg -c \
  "select t.tabular_namespace_name, t.name, t.typ, t.fs_location,
          (select count(*) from table_snapshot s where s.table_id = t.tabular_id) as snapshots
   from tabular t;"
```

```
 tabular_namespace_name |    name    |  typ  |             fs_location              | snapshots
------------------------+------------+-------+--------------------------------------+-----------
 {lab1}                 | customers2 | table | warehouse/iceberg/<table-uuid>       |         5
(1 row)
```

Warehouse registration (S3 endpoint, bucket, vending flag) as stored in Postgres:

```bash
docker compose exec -T postgres psql -U postgres -d iceberg -c \
  "select warehouse_name,
          storage_profile->>'bucket'      as bucket,
          storage_profile->>'endpoint'    as endpoint,
          storage_profile->>'sts-enabled' as sts_enabled
   from warehouse;"
```

```
 warehouse_name |  bucket   |             endpoint              | sts_enabled
----------------+-----------+-----------------------------------+-------------
 demo           | warehouse | http://host.docker.internal:9000/ | true
(1 row)
```

You can also point any client at it directly: host `localhost`, port `5432`,
db `iceberg`, user `postgres`, password `postgres`.

## Step 6 — Prove the data files are in RustFS

```bash
docker compose run --rm --no-deps rustfs-init \
  "mc alias set rustfs http://rustfs:9000 lakehouse-admin lakehouse-admin-secret > /dev/null && \
   mc ls --recursive rustfs/warehouse/iceberg/"
```

```
[2026-09-20 20:20:48 UTC]   636B STANDARD <table-uuid>/data/01a0c07a-7f98-....parquet       <- data rows
[2026-09-20 20:20:48 UTC]   608B STANDARD <table-uuid>/data/01a0c07a-7fe1-....parquet
[2026-09-20 20:20:48 UTC]   567B STANDARD <table-uuid>/data/01a0c07a-7fe1-....parquet
[2026-09-20 20:20:48 UTC]   602B STANDARD <table-uuid>/data/01a0c07a-80ba-....parquet
[2026-09-20 20:20:48 UTC]   884B STANDARD <table-uuid>/data/1329fd06-...-deletes.parquet     <- MERGE deletes
[2026-09-20 20:20:48 UTC]   869B STANDARD <table-uuid>/data/232c4ed6-...-deletes.parquet
[2026-09-20 20:20:48 UTC]   333B STANDARD <table-uuid>/metadata/00000-....gz.metadata.json  <- Iceberg table metadata
[2026-09-20 20:20:48 UTC]   594B STANDARD <table-uuid>/metadata/00001-....gz.metadata.json
[2026-09-20 20:20:48 UTC]  2.3KiB STANDARD <table-uuid>/metadata/<manifest>-m0.avro         <- manifests
[2026-09-20 20:20:48 UTC]  1.5KiB STANDARD <table-uuid>/metadata/snap-<snapshot-id>-....avro<- snapshot manifests
```

22 objects were created by the exercise run above (4 data files, 2 delete
files, 6 `metadata.json` versions, 5 snapshot manifests, 5 manifests). The same
list is visible in the RustFS web console at <http://localhost:9001> →
*Buckets* → `warehouse` → `iceberg/`.

Everything is consistent: **metadata pointer in Postgres → Iceberg metadata in
RustFS → Parquet data files in RustFS**.

## Step 7 — Re-run, reset, tear down

```bash
# re-run the exercise on the existing table (idempotent: the seed is re-created)
python python/run_merge_exercise.py

# full reset: drop RustFS objects + Postgres metadata, start clean, re-run
docker compose down -v
docker compose up -d
python python/run_merge_exercise.py

# stop and keep the data
docker compose down

# stop and delete everything (containers, networks, volumes)
docker compose down -v
```

`docker compose up -d` re-runs the one-shot init jobs (`rustfs-init`,
`lakekeeper-migrate`, `lakekeeper-bootstrap`, `lakekeeper-warehouse-init`).
They are written to be idempotent — a re-run prints
`bootstrap -> HTTP 400 / catalog already bootstrapped` and
`warehouse create -> HTTP 400 / warehouse 'demo' already exists`, both treated
as success.

---

## Troubleshooting (every failure below was hit while building this runbook)

| Symptom | Cause | Fix |
| --- | --- | --- |
| `Request to 'http://localhost:8181/v1/config?warehouse=demo' returned a non-200 status code (NotFound_404)` | Lakekeeper serves its REST API under `/catalog` | use `ENDPOINT 'http://localhost:8181/catalog'` in `ATTACH` |
| `IO Error: Could not resolve hostname error for HTTP PUT to 'http://rustfs:9000/...'` | the warehouse profile's S3 endpoint must also resolve **from your host**, and `rustfs` only exists inside the Docker network | set `"endpoint": "http://host.docker.internal:9000"` (already the default here); on Linux keep `extra_hosts: host.docker.internal:host-gateway` (already in the compose file) or add the name to `/etc/hosts` |
| `AccessDenied: Access Denied ... (HTTP code 403)` on `INSERT`/`MERGE` | warehouse has `sts-enabled: false`, so Lakekeeper vends only `s3.endpoint`/`s3.path-style-access` (**no keys**) and the request is unsigned | set `"sts-enabled": true` plus `"sts-role-arn"` and `"sts-endpoint"` (requires an S3 server with STS `AssumeRole` — RustFS has it) |
| `CatalogConfig required property 'defaults' is missing` during `ATTACH` | DuckDB too old for the catalog's config payload | upgrade DuckDB (`pip install -U "duckdb>=1.5.3"`) |
| `MERGE INTO` is a parse error / "not supported" | DuckDB < 1.5.3 | upgrade: `MERGE INTO` against Iceberg landed in v1.5.3 |
| `service "lakekeeper-bootstrap" didn't complete successfully: exit 1` | init job got an unexpected HTTP status (typically: catalog wasn't healthy yet on the very first run) | run `docker compose up -d` again (jobs are idempotent) and inspect `docker compose logs lakekeeper-bootstrap lakekeeper-warehouse-init` |
| `pull access denied for minio/mc` | MinIO images moved off Docker Hub | the compose file uses `quay.io/minio/mc:latest` |
| RustFS exits with permission errors on `/data` | bind-mounting a host directory: the container runs as uid `10001` | keep the named volume (default) or `chown -R 10001:10001 /path/on/host` |
| `failed to connect to the docker API ... dockerDesktopLinuxEngine` | Docker Desktop engine not running | start Docker Desktop; wait until `docker info` succeeds |
| Ports already in use | something else holds `5432`/`8181`/`9000`/`9001` | stop that process or change the published ports in `docker-compose.yml` |
| PowerShell prints red `NativeCommandError` for normal compose output | Docker writes progress to stderr | harmless; use `cmd /c "docker compose up -d"` or ignore |

Extra diagnostics that turned out to be the fastest way to localise problems:

```bash
docker compose logs lakekeeper | Select-String -Pattern "storage|sts|error"   # catalog side
curl -s -H "Authorization: Bearer dummy" \
  "http://localhost:8181/catalog/v1/<prefix>/namespaces/lab1/tables/customers2/credentials"
#   ^ shows exactly what Lakekeeper vends to query engines (keys present or not)
```

---

## Verified setup for this runbook

| Component | Version / detail |
| --- | --- |
| Host | Windows, Docker Desktop, engine `29.8.0`, Compose `v5.5.1` |
| Python | 3.14.7, `duckdb` **1.5.5** (iceberg extension `45163a28`, httpfs `827222f`) |
| Object storage | `rustfs/rustfs:1.0.0`, bucket `warehouse`, credentials `lakehouse-admin` / `lakehouse-admin-secret` |
| Catalog DB | `postgres:17`, database `iceberg`, schema `public` |
| Catalog server | `quay.io/lakekeeper/catalog:v0.13.5`, warehouse `demo`, REST base `http://localhost:8181/catalog` |
| Helper images | `quay.io/minio/mc:latest` (bucket + inspection), `curlimages/curl:latest` (bootstrap/warehouse init) |

### Credentials, ports and names used everywhere

| Item | Value |
| --- | --- |
| RustFS access key / secret | `lakehouse-admin` / `lakehouse-admin-secret` |
| Postgres | `postgres` / `postgres`, db `iceberg`, port `5432` |
| Warehouse / bucket / key prefix | `demo` / `warehouse` / `iceberg` |
| S3 region | `us-east-1` (must be consistent in RustFS profile and the S3 secret) |
| S3 endpoint | `host.docker.internal:9000` (host + container), `http://rustfs:9000` for in-network STS |
| Catalog REST | `http://localhost:8181/catalog` (management API under `/management/v1/...`) |

### Limitations / notes

* Local, single-node, unsecured catalog: **any** bearer token is accepted and
  there is no auth or authorization. Do not expose ports 9000/9001/8181/5432
  beyond your machine. For a secured setup add OIDC + OpenFGA (or Cedar) to
  Lakekeeper and turn on RustFS IAM users.
* `MERGE INTO`, `UPDATE` and `DELETE` on Iceberg from DuckDB are
  **merge-on-read only** (positional deletes / deletion vectors); copy-on-write
  is not implemented, and operations on *sorted* tables are rejected.
  Iceberg v3 tables use deletion vectors (Puffin) instead of Parquet deletes.
* Requires **DuckDB ≥ 1.5.3**; older versions can read REST catalogs but cannot
  run `MERGE INTO` on Iceberg tables.
* Switching storage backends later means re-creating the warehouse: most S3
  profile fields (`bucket`, `endpoint`, `key-prefix`, …) are immutable.

### Porting this back to AWS

Everything above is the same code path the article uses — only the catalog
changes:

```sql
-- AWS S3 Tables (as in the article)
CREATE OR REPLACE SECRET s3_dev (TYPE s3, PROVIDER credential_chain, CHAIN 'config', PROFILE 'dev', REGION 'us-east-1');
ATTACH 'arn:aws:s3tables:us-east-1:<account>:bucket/<bucket>'
  AS s3_tables_db (TYPE iceberg, ENDPOINT_TYPE s3_tables);
-- or a Glue catalog: TYPE iceberg, ENDPOINT_TYPE glue
```

then run the same `CREATE TABLE` / `MERGE INTO` statements from `sql/02`–`sql/05`.
