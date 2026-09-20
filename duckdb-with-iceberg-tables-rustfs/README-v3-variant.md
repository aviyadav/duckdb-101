# Runbook (exercise 2): DuckDB reads Iceberg **format v3** + **VARIANT** — fully local edition

Local reproduction of
**["Can DuckDB Read Iceberg V3 VARIANT from Amazon S3 Tables? Quick Test"](https://medium.com/@shahsoumil519/can-duckdb-read-iceberg-v3-variant-from-amazon-s3-tables-quick-test-a191ab70bf80)**
(Soumil Shah, May 2026). The article attaches an **AWS S3 Tables** catalog and runs four
checks against `s3_tables_db.test.gh_shred` (`id` int64 + `v` variant, GitHub Archive events).

This exercise reuses **the exact same local stack as exercise 1**
(`docker-compose.yml`: RustFS + Lakekeeper + PostgreSQL) — no new infrastructure, just a second
set of tables, SQL files and a runner.

| Article (AWS) | This exercise (local) |
| --- | --- |
| S3 Tables bucket (storage) | **RustFS** bucket `warehouse`, prefix `iceberg/` |
| S3 Tables catalog (`ENDPOINT_TYPE s3_tables`) | **Lakekeeper** REST catalog on `http://localhost:8181/catalog`, metadata in **PostgreSQL** |
| `s3_tables_db.test.gh_shred` | `lake.test.gh_shred` (same namespace + table name: `test.gh_shred`) |
| `INSTALL aws; CALL load_aws_credentials(); CREATE SECRET (TYPE s3, PROVIDER credential_chain)` | Lakekeeper **vends STS credentials** for RustFS, so DuckDB needs no AWS credential chain |
| `FORCE INSTALL iceberg FROM core_nightly` ("the version gotcha") | **not needed**: stable DuckDB **≥ 1.5.3** already reads *and writes* v3 |
| GitHub Archive data ingested by an external pipeline | deterministic synthetic GH-Archive-shaped events, plus a JSON-file ingestion path you can point at real GH Archive dumps |

### Files added for this exercise

```
sql-v3-variant/01_secrets_and_attach.sql        # extensions, secrets, ATTACH (same stack as exercise 1)
sql-v3-variant/02_create_v3_table.sql           # CREATE TABLE ... WITH ('format-version' = 3), VARIANT + TIMESTAMP_NS
sql-v3-variant/03_ingest_github_events.sql      # 12 GH-Archive-shaped events (JSON text -> VARIANT)
sql-v3-variant/04_test1_read_one_row.sql        # article Test 1
sql-v3-variant/05_test2_dot_notation.sql        # article Test 2
sql-v3-variant/06_test3_variant_typeof.sql      # article Test 3
sql-v3-variant/07_test4_aggregations.sql        # article Test 4
sql-v3-variant/08_group_by_event_type.sql       # extra: aggregate by a VARIANT field
sql-v3-variant/09_variant_shapes.sql            # extra: per-row shapes (self-describing values)
sql-v3-variant/10_variant_helpers.sql           # extra: variant_extract / variant_normalize
sql-v3-variant/11_variant_any_type.sql          # extra: VARIANT holds scalars, arrays, objects
sql-v3-variant/12_v3_deletion_vectors.sql       # extra: v3 DELETE -> Puffin deletion vector
sql-v3-variant/13_v3_metadata_as_variant.sql    # extra: table metadata itself exposed as VARIANT
sql-v3-variant/14_v3_time_travel.sql            # extra: read the pre-delete snapshot
sql-v3-variant/15_verify_v3_files.sql           # extra: physical files (parquet + puffin)
sql-v3-variant/16_ingest_from_json_file.sql     # ingestion path for real NDJSON / GH Archive dumps
sql-v3-variant/17_variant_parquet_shredding.sql # extra: how VARIANT is shredded in Parquet
data/sample_github_events.ndjson                # 3-event sample used by step 16
python/run_v3_variant_exercise.py               # runs all of the above in order
```

Exercise 1 (MERGE on Iceberg) is untouched: `sql/`, `python/run_merge_exercise.py`, `README.md`.
Both exercises share one warehouse (`demo`) and one bucket, using different namespaces
(`lab1` for exercise 1, `test` for exercise 2).

### Prerequisites

* The stack from exercise 1 is running: `docker compose up -d` (see `README.md`, steps 1–2).
* `duckdb` Python package **≥ 1.5.3** (`python -m pip install "duckdb>=1.5.3"`) — v3 `VARIANT` support.
* Nothing else: no AWS account, no `core_nightly` extension, no internet access.

---

## Step 1 — Make sure the local lakehouse is up

```bash
docker compose up -d
docker compose ps          # postgres + lakekeeper healthy, rustfs up
```

## Step 2 — Run the v3 VARIANT exercise

```bash
python python/run_v3_variant_exercise.py
```

Real output (DuckDB 1.5.5, iceberg extension `45163a28`, 2026-09-20):

```
Exercise 2: Iceberg format v3 + VARIANT (RustFS storage, PostgreSQL catalog)
  extension_name | extension_version
  ---------------+------------------
  httpfs         | 827222f
  iceberg        | 45163a28
  json           | v1.5.5
  (3 rows)
duckdb (pip) version: 1.5.5
catalog endpoint: http://localhost:8181/catalog
S3 endpoint:      host.docker.internal:9000
sample events:    .../data/sample_github_events.ndjson

=== 01_secrets_and_attach.sql ===================================
  lake | lab1 | customers2   | ... (exercise 1's table)
  lake | test | gh_shred     | ...

=== 02_create_v3_table.sql ======================================
  column_name | column_type  | null | key | default | extra
  ------------+--------------+------+-----+---------+------
  id          | BIGINT       | YES  |     |         |
  v           | VARIANT      | YES  |     |         |
  created_at  | TIMESTAMP_NS | YES  |     |         |
  (3 rows)

=== 03_ingest_github_events.sql =================================
  ingested | with_org | with_action
  ---------+----------+------------
  12       | 8        | 6
  (1 row)
```

Both v3 data types are in place: `VARIANT` plus `TIMESTAMP_NS` (nanosecond timestamps are
another v3 feature; the article's table only had `id` + `v`).

### The four article tests

```
=== 04_test1_read_one_row.sql ===================================
  id | v
  ---+-------------------------------------------------------------------------
  1  | {'actor': {'id': 101, 'login': 'alice'}, 'created_at': '2026-05-23T22:31:07Z',
       'payload': {'ref': 'refs/heads/main', 'size': 2}, 'public': True,
       'repo': {'name': 'duckdb/duckdb'}, 'type': 'PushEvent'}
  (1 row)                                                    <-- Test 1: PASS

=== 05_test2_dot_notation.sql ===================================
  id | event_type        | created_at           | actor | org
  ---+-------------------+----------------------+-------+-----------
  1  | PushEvent         | 2026-05-23T22:31:07Z | alice |
  2  | WatchEvent        | 2026-05-23T22:32:11Z | bob   |
  3  | IssuesEvent       | 2026-05-23T22:33:02Z | carol | rustfs
  4  | PullRequestEvent  | 2026-05-23T22:34:45Z | dave  | lakekeeper
  5  | CreateEvent       | 2026-05-23T22:35:30Z | erin  | duckdb
  6  | ReleaseEvent      | 2026-05-23T22:36:10Z | frank | rustfs
  7  | IssueCommentEvent | 2026-05-23T22:37:52Z | grace | apache
  8  | DeleteEvent       | 2026-05-23T22:38:20Z | heidi | example
  9  | MemberEvent       | 2026-05-23T22:39:05Z | ivan  | lakekeeper
  10 | ForkEvent         | 2026-05-23T22:40:33Z | judy  |
  (10 rows)                                                  <-- Test 2: PASS

=== 06_test3_variant_typeof.sql =================================
  id | variant_type
  ---+------------------------------------------------------------
  1  | OBJECT(actor, created_at, payload, public, repo, type)
  2  | OBJECT(actor, created_at, payload, public, repo, type)
  3  | OBJECT(actor, created_at, org, payload, public, repo, type)
  4  | OBJECT(actor, created_at, org, payload, public, repo, type)
  5  | OBJECT(actor, created_at, org, payload, public, repo, type)
  (5 rows)                                                   <-- Test 3: PASS

=== 07_test4_aggregations.sql ===================================
  total | with_org | with_action
  ------+----------+------------
  12    | 8        | 6
  (1 row)                                                    <-- Test 4: PASS
```

Test 4 is the one that proves real per-value field access: only 8 of the 12 events have an
`org`, and only 6 have a payload `action`, and DuckDB counts them correctly while reading the
shredded column.

### The extra steps (v3 features the article did not cover)

```
=== 08_group_by_event_type.sql ==================================
  event_type        | events | with_org | with_action
  ------------------+--------+----------+------------
  PushEvent         | 2      | 1        | 0
  CreateEvent       | 1      | 1        | 0
  DeleteEvent       | 1      | 1        | 0
  ForkEvent         | 1      | 0        | 0
  IssueCommentEvent | 1      | 1        | 1
  IssuesEvent       | 1      | 1        | 1
  MemberEvent       | 1      | 1        | 1
  PublicEvent       | 1      | 0        | 0
  PullRequestEvent  | 1      | 1        | 1
  ReleaseEvent      | 1      | 1        | 1
  WatchEvent        | 1      | 0        | 1
  (11 rows)

=== 09_variant_shapes.sql =======================================
  variant_shape                                               | events | first_id | last_id
  ------------------------------------------------------------+--------+----------+--------
  OBJECT(actor, created_at, org, payload, public, repo, type) | 8      | 3        | 12
  OBJECT(actor, created_at, payload, public, repo, type)      | 4      | 1        | 11
  (2 rows)

=== 10_variant_helpers.sql ======================================
  id | actor_variant                 | actor_as_text               | repo_name      | normalized_type
  ---+-------------------------------+-----------------------------+----------------+------------------------
  1  | {'id': 101, 'login': 'alice'} | {'id': 101, 'login': alice} | duckdb/duckdb  | OBJECT(actor, created_at, payload, public, repo, type)
  2  | {'id': 102, 'login': 'bob'}   | {'id': 102, 'login': bob}   | apache/iceberg | OBJECT(actor, created_at, payload, public, repo, type)
  3  | {'id': 103, 'login': 'carol'} | {'id': 103, 'login': carol} | rustfs/rustfs  | OBJECT(actor, created_at, org, payload, public, repo, type)
  (3 rows)

=== 11_variant_any_type.sql =====================================
  int_variant | text_variant | array_variant | object_variant | null_variant
  ------------+--------------+---------------+----------------+-------------
  INT32       | VARCHAR      | ARRAY(3)      | OBJECT(k)      | VARIANT_NULL
  (1 row)

=== 12_v3_deletion_vectors.sql ==================================
  rows_after_delete | deleted_id_visible | delete_file_format
  ------------------+--------------------+-------------------
  11                | 0                  | puffin
  (1 row)
```

```
=== 13_v3_metadata_as_variant.sql ===============================
  format_version | current_snapshot_id | last_sequence_number | location
  ---------------+---------------------+----------------------+---------------------------------------------
  3              | 159222256799072724  | 2                    | s3://warehouse/iceberg/<table-uuid>
  (1 row)            (also: variant_typeof(metadata.schemas) = ARRAY(1))

=== 14_v3_time_travel.sql =======================================
  first_snapshot_id   | rows_at_first_snapshot | rows_now
  --------------------+------------------------+---------
  7392929923911795511 | 12                     | 11
  (1 row)

=== 15_verify_v3_files.sql ======================================
  manifest_content | content          | file_format | record_count | file_path
  -----------------+------------------+-------------+--------------+----------------------------------------------
  DATA             | EXISTING         | parquet     | 12           | s3://warehouse/iceberg/<uuid>/data/<uuid>.parquet
  DELETE           | POSITION_DELETES | puffin      | 1            | s3://warehouse/iceberg/<uuid>/data/<uuid>-deletes.puffin
  (2 rows)

=== 16_ingest_from_json_file.sql ================================
  id | variant_type                                                    | event_type  | actor | created_at
  ---+-----------------------------------------------------------------+-------------+-------+--------------------
  1  | OBJECT(actor, created_at, id, org, payload, public, repo, type) | PushEvent   | mona  | 2026-05-23 22:50:01
  2  | OBJECT(actor, created_at, id, org, payload, public, repo, type) | WatchEvent  | nate  | 2026-05-23 22:51:33
  3  | OBJECT(actor, created_at, id, org, payload, public, repo, type) | IssuesEvent | olga  | 2026-05-23 22:52:57
  (3 rows)

=== 17_variant_parquet_shredding.sql ============================
  name          | type       | repetition_type | num_children
  --------------+------------+-----------------+-------------
  duckdb_schema |            | REQUIRED        | 3
  id            | INT64      | OPTIONAL        |
  v             |            | OPTIONAL        | 3
  metadata      | BYTE_ARRAY | REQUIRED        |
  value         | BYTE_ARRAY | OPTIONAL        |
  typed_value   |            | OPTIONAL        | 7
  type          |            | OPTIONAL        | 2
  ...
  actor         |            | OPTIONAL        | 2
  ...
  login         |            | OPTIONAL        | 2
  (20 rows)

Done: Iceberg v3 VARIANT read back from RustFS via the PostgreSQL-backed catalog.
```

What the extras show:

* **09–11** — `VARIANT` is self-describing per row: two different object shapes in the same
  column, and any value type (INT32 / VARCHAR / ARRAY / OBJECT / NULL) is representable.
* **12** — v3 semantics: the `DELETE` is stored as a **binary deletion vector in a `.puffin`
  file**, not as the `-deletes.parquet` positional deletes that exercise 1's v2 table produced.
* **13** — the catalog's `LoadTable` response exposes the Iceberg **table metadata as VARIANT**,
  so `metadata."format-version"` (here `3`) can be read with dot notation.
* **14** — time travel to the first snapshot returns 12 rows while the current table has 11
  (the deleted event) — the deletion vector is respected.
* **17** — here is the "shred" in `gh_shred`: DuckDB writes `VARIANT` to Parquet **shredded**
  (`metadata` / `value` / `typed_value` groups, with typed sub-columns such as `actor.login`),
  which is what makes dot notation and predicate pushdown cheap.

---

## Re-run / reset / tear down

```bash
# re-run (repeatable: the v3 table is dropped and re-created)
python python/run_v3_variant_exercise.py

# re-run both exercises on a completely clean stack
docker compose down -v
docker compose up -d
python python/run_merge_exercise.py
python python/run_v3_variant_exercise.py

# stop / wipe (same as exercise 1)
docker compose down
docker compose down -v
```

## Verified setup (same stack as exercise 1)

DuckDB **1.5.5** (`iceberg` `45163a28`, `httpfs` `827222f`, `json` `v1.5.5`),
`rustfs/rustfs:1.0.0`, `quay.io/lakekeeper/catalog:v0.13.5`, `postgres:17`,
warehouse `demo` (bucket `warehouse`, key prefix `iceberg/`). DuckDB ≥ 1.5.3 required.
All 17 steps above ran green on the machine this repo was created on
(Windows + Docker Desktop, 2026-09-20).

For the AWS version of this exercise (attach S3 Tables by ARN, read the real
`s3_tables_db.test.gh_shred`), see the article:
https://medium.com/@shahsoumil519/can-duckdb-read-iceberg-v3-variant-from-amazon-s3-tables-quick-test-a191ab70bf80



