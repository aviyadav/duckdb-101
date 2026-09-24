# Run book — driving the cluster from your local Linux host

Step-by-step instructions for running Python on your **host** against the DuckDB workers in
Docker, in both server modes. Every step lists the command, how to verify it, and the output
you should expect.

Work top to bottom. **Part 0** is shared, then pick **Part 1** (HTTP mode) or **Part 2**
(Quack mode). The two parts are independent — you do not need to run Part 1 before Part 2.

Related documents:

- [`README.md`](README.md) — project overview and the in-container runbooks
- [`local/README.md`](local/README.md) — reference for the host-side client scripts
- [`run-book.md`](run-book.md) — the original in-container run book

---

## Contents

- [Part 0 — Shared preparation](#part-0--shared-preparation)
- [Part 1 — HTTP mode](#part-1--http-mode-server_modehttp)
- [Part 2 — Quack mode](#part-2--quack-mode-server_modequack)
- [Part 3 — Troubleshooting](#part-3--troubleshooting)
- [Appendix — Cheat sheet](#appendix--cheat-sheet)

---

## Part 0 — Shared preparation

### 0.1 Go to the project root

Every command in this run book assumes you are in the directory containing `docker-compose.yml`.

```bash
cd ~/codebase/python-base/duckdb-local-quack-docker
```

**Verify**

```bash
ls docker-compose.yml local/http_client.py local/quack_client.py
```

All three paths should be listed.

### 0.2 Check Docker and the compose file

```bash
docker compose version
docker compose config --services
```

**Expected** — the four service names `worker-1`, `worker-2`, `worker-3`, `coordinator`.

### 0.3 Build the image

```bash
docker compose build
```

**Verify** — the build finishes without error. All four services share one image, so this only
builds once.

### 0.4 Load the host environment

```bash
source local/.env.local
```

**Verify**

```bash
echo "$WORKER_1_ENDPOINT $WORKER_2_ENDPOINT $WORKER_3_ENDPOINT"
```

**Expected**

```text
localhost:9491 localhost:9492 localhost:9493
```

These are the same variable names the in-container coordinator reads, so the exports work for
`local/*.py` **and** for `app/related_cluster_sql.py` run from the host.

> `source` only affects the current shell. Re-run it in every new terminal.

### 0.5 Which mode are the workers in?

```bash
docker compose config | grep SERVER_MODE
```

**Expected** — either `SERVER_MODE: http` or `SERVER_MODE: quack`. This decides which part of
the run book you follow. The value comes from `.env` in the project root.

### Port reference

| Worker | Host endpoint | Table |
| --- | --- | --- |
| `worker-1` | `localhost:9491` | `sales` |
| `worker-2` | `localhost:9492` | `customers` |
| `worker-3` | `localhost:9493` | `products` |

---

## Part 1 — HTTP mode (`SERVER_MODE=http`)

The workers run `app/http_sql_server.py`: a small standard-library HTTP server exposing
`GET /health` and `POST /query`. **No pip installs are needed on the host** — `http_client.py`
is stdlib only.

### H1. Select HTTP mode and start the workers

Set in `.env`:

```dotenv
SERVER_MODE=http
```

Then:

```bash
docker compose up -d worker-1 worker-2 worker-3
```

**Verify**

```bash
docker compose ps
```

**Expected** — all three workers `Up`. If any show `Restarting`, see
[Troubleshooting](#part-3--troubleshooting).

### H2. Confirm seeding finished

```bash
docker compose logs worker-1 | tail -20
```

**Expected** on first boot:

```text
Seeding worker 1 with 100000 rows into /data/worker.duckdb
Seeding worker 1 table 'sales' with 100000 rows into /data/worker.duckdb
Created sales with 100000 rows
Starting local HTTP SQL server mode
HTTP SQL server listening on 0.0.0.0:9494, db=/data/worker.duckdb
```

On later boots the seeding lines are skipped — the `/data/worker.duckdb.seeded` marker already
exists. Check `worker-2` (`customers`) and `worker-3` (`products`) the same way.

### H3. Health-check all three workers from the host

```bash
python local/http_client.py --health
```

**Expected**

```text
worker-1   http://localhost:9491/health  OK    {'status': 'ok', 'db': '/data/worker.duckdb'}
worker-2   http://localhost:9492/health  OK    {'status': 'ok', 'db': '/data/worker.duckdb'}
worker-3   http://localhost:9493/health  OK    {'status': 'ok', 'db': '/data/worker.duckdb'}
```

Exit code is `0` when all three respond. No dependency on `curl` or `requests`.

Equivalent raw check, if you prefer:

```bash
curl -s localhost:9491/health
```

### H4. First read query

```bash
python local/http_client.py --query "worker-1=SELECT COUNT(*) AS n FROM sales"
```

**Expected**

```text
=== worker-1  http://localhost:9491/query ===
SELECT COUNT(*) AS n FROM sales

n
------
100000
```

### H5. Read from all three workers in one command

```bash
python local/http_client.py \
  --query "worker-1=SELECT COUNT(*) AS n FROM sales" \
  --query "worker-2=SELECT COUNT(*) AS n FROM customers" \
  --query "worker-3=SELECT COUNT(*) AS n FROM products"
```

**Verify** — three `=== worker-N ===` blocks, each returning `100000`.

> `http_client.py` runs its queries sequentially. For the *concurrent* fan-out with barrier
> timing, use the coordinator in [H10](#h10-run-the-existing-coordinator-from-the-host).

### H6. Aggregation across a group

```bash
python local/http_client.py \
  --query "worker-1=SELECT sale_status, COUNT(*) AS count_star FROM sales GROUP BY sale_status ORDER BY sale_status" \
  --query "worker-2=SELECT country, COUNT(*) AS count_star FROM customers GROUP BY country ORDER BY country" \
  --query "worker-3=SELECT category, COUNT(*) AS count_star FROM products GROUP BY category ORDER BY category"
```

**Expected** — five `sale_status` rows, six `country` rows, six `category` rows.

### H7. Writes: clear the target range first

Writes need `--allow-write`. Without it the worker opens the database `read_only=True` and the
statement fails.

```bash
python local/http_client.py --allow-write \
  --query "worker-1=DELETE FROM sales WHERE sale_id BETWEEN 30000001 AND 30000020"
```

**Expected** — a single `Count` column. On a fresh 100 000-row seed the range
30000001–30000020 does not exist yet, so the count is `0`: this statement is a no-op that
guarantees a clean slate for the next step. Re-running it after H8 reports `3`.

### H8. Writes: insert rows and read them back

```bash
python local/http_client.py --allow-write \
  --query "worker-1=INSERT INTO sales (sale_id, customer_id, product_id, quantity, sales_channel, payment_method, sale_status, sale_date, sold_unit_price, discount_pct) VALUES (30000001, 1, 1, 1, 'online', 'card', 'completed', DATE '2026-09-24', 10.00, 0.00)" \
  --query "worker-1=INSERT INTO sales (sale_id, customer_id, product_id, quantity, sales_channel, payment_method, sale_status, sale_date, sold_unit_price, discount_pct) VALUES (30000002, 2, 2, 2, 'store', 'bank_transfer', 'processing', DATE '2026-09-24', 20.00, 0.00)" \
  --query "worker-1=INSERT INTO sales (sale_id, customer_id, product_id, quantity, sales_channel, payment_method, sale_status, sale_date, sold_unit_price, discount_pct) VALUES (30000003, 3, 3, 3, 'marketplace', 'wallet', 'shipped', DATE '2026-09-24', 30.00, 0.00)"
```

Then read them back **without** `--allow-write`, proving the reads take the read-only path:

```bash
python local/http_client.py \
  --query "worker-1=SELECT sale_id, sale_status, quantity FROM sales WHERE sale_id BETWEEN 30000001 AND 30000020 ORDER BY sale_id"
```

**Expected** — the three inserted rows. Each `INSERT` also returns a `Count` column; the exact
shape of a write result comes from `_fetch()` in `app/http_sql_server.py`, which labels
statements that produce no result set as `Count`.

### H9. DDL: build and read an aggregate table

```bash
python local/http_client.py --allow-write \
  --query "worker-1=CREATE OR REPLACE TABLE sales_agg AS SELECT sale_status, COUNT(*) AS sale_count FROM sales GROUP BY sale_status" \
  --query "worker-2=CREATE OR REPLACE TABLE customers_agg AS SELECT country, COUNT(*) AS customer_count FROM customers GROUP BY country" \
  --query "worker-3=CREATE OR REPLACE TABLE products_agg AS SELECT category, COUNT(*) AS product_count FROM products GROUP BY category"
```

```bash
python local/http_client.py \
  --query "worker-1=SELECT * FROM sales_agg ORDER BY sale_count DESC" \
  --query "worker-2=SELECT * FROM customers_agg ORDER BY customer_count DESC" \
  --query "worker-3=SELECT * FROM products_agg ORDER BY product_count DESC"
```

**Verify** — each worker returns its own aggregate table, confirming DDL ran against three
separate databases.

### H10. Run the existing coordinator from the host

`app/related_cluster_sql.py` is stdlib-only in HTTP mode, so it runs on the host directly and
gives you the real concurrent fan-out with barrier timing.

```bash
SERVER_MODE=http python app/related_cluster_sql.py \
  --query "worker-1=SELECT sale_status, COUNT(*) AS count_star FROM sales GROUP BY sale_status ORDER BY sale_status" \
  --query "worker-2=SELECT country, COUNT(*) AS count_star FROM customers GROUP BY country ORDER BY country" \
  --query "worker-3=SELECT category, COUNT(*) AS count_star FROM products GROUP BY category ORDER BY category"
```

**Expected**

```text
Waiting for worker-1 at localhost:9491 ...
Waiting for worker-2 at localhost:9492 ...
Waiting for worker-3 at localhost:9493 ...

Concurrent remote queries
worker     table      start_offset_ms duration_seconds
---------- ---------- --------------- ------------------
worker-3   query-3              0.976              0.442
worker-1   query-1              1.123              0.431
worker-2   query-2              1.325              0.455

Start spread: 0.349 ms
```

Note the endpoints now say `localhost:949N` — that is `local/.env.local` doing its job.
**What proves concurrency:** `Start spread` is a fraction of a millisecond, so all three
fragments left the host at effectively the same instant.

### H11. Use it as a library

```bash
python - <<'PY'
import sys
sys.path.insert(0, "local")

from http_client import HttpClient

for worker, table in [("worker-1", "sales"), ("worker-2", "customers"), ("worker-3", "products")]:
    result = HttpClient(worker).query(f"SELECT COUNT(*) AS n FROM {table}")
    print(worker, result["columns"], result["rows"])
PY
```

**Expected**

```text
worker-1 ['n'] [[100000]]
worker-2 ['n'] [[100000]]
worker-3 ['n'] [[100000]]
```

### H12. Tear down

```bash
docker compose down        # keep the seeded volumes
docker compose down -v     # delete the data too; next boot re-seeds
```

---

## Part 2 — Quack mode (`SERVER_MODE=quack`)

The workers run `app/quack_server.py`, which loads the real DuckDB **Quack** extension and calls
`quack_serve('quack:0.0.0.0:9494', allow_other_hostname => true, token => ...)`. From the host
you attach to them as remote catalogs, so remote tables behave like local ones.

This track needs `duckdb` installed locally, and the version **must** match the image.

### Q1. Select Quack mode and recreate the workers

Set in `.env`:

```dotenv
SERVER_MODE=quack
```

Then:

```bash
docker compose down
docker compose up -d --build worker-1 worker-2 worker-3
```

**Verify**

```bash
docker compose config | grep SERVER_MODE
docker compose ps
```

**Expected** — `SERVER_MODE: quack`, and all three workers `Up` (not `Restarting`).

> `--build` matters whenever you have edited anything under `app/`. The code is copied into the
> image at build time; there is no bind mount.

### Q2. Confirm the Quack server started

```bash
docker compose logs worker-1 | tail -20
```

**Expected**

```text
Starting Quack server mode
Starting Quack server on quack:0.0.0.0:9494, db=/data/worker.duckdb
  uri: quack:0.0.0.0:9494
  url: http://0.0.0.0:9494
  auth_token: <redacted, set in QUACK_TOKEN>
```

`quack_serve` returns the listen URI, the HTTP URL and the effective token; exact column names
can vary between Quack builds. Repeat for `worker-2` and `worker-3`.

### Q3. Create a virtualenv and install duckdb

```bash
python -m venv .venv
. .venv/bin/activate
pip install -r local/requirements.txt
```

**Verify**

```bash
python -c "import duckdb; print(duckdb.__version__)"
```

**Expected**

```text
1.5.5
```

This must match `requirements.txt` in the project root. DuckDB extensions are built per version,
so a mismatch makes `INSTALL quack` / `LOAD quack` fail.

### Q4. Load Quack locally and attach every worker

```bash
python local/quack_client.py --check
```

**Expected**

```text
host duckdb 1.5.5, quack loaded, token set
attached worker-1   as w1  (quack:localhost:9491)
attached worker-2   as w2  (quack:localhost:9492)
attached worker-3   as w3  (quack:localhost:9493)
worker-1   SELECT 1 -> [(1,)]
worker-2   SELECT 1 -> [(1,)]
worker-3   SELECT 1 -> [(1,)]
```

This single step proves four things: the extension installed on the host, the token is accepted,
all three published ports are reachable, and remote SQL executes. The first run downloads the
extension into `~/.duckdb/extensions`, so it needs internet and takes a few seconds.

### Q5. First read query

```bash
python local/quack_client.py --query "worker-1=SELECT COUNT(*) AS n FROM sales"
```

**Expected**

```text
host duckdb 1.5.5, quack loaded, token set

=== worker-1 ===
SELECT COUNT(*) AS n FROM sales

n
------
100000
```

### Q6. List what the remote catalogs expose

```bash
python local/quack_client.py --attach-all --tables
```

**Expected**

```text
database_name  schema_name  table_name
-------------  -----------  ----------
w1             main         customers_agg
w1             main         sales
w2             main         customers
w3             main         products
```

(Exact rows depend on what earlier steps created.) If the list looks empty, fall back to
`--attach-all --sql "SHOW ALL TABLES"`.

### Q7. Remote tables vs pushed-down SQL

Quack gives you two ways to run something remotely. Both are worth seeing once.

```bash
python local/quack_client.py --attach-all \
  --sql "SELECT sale_status, COUNT(*) AS n FROM w1.sales GROUP BY 1 ORDER BY 1" \
  --sql "FROM w1.query('SELECT sale_status, COUNT(*) AS n FROM sales GROUP BY 1 ORDER BY 1')"
```

**Verify** — both blocks return identical rows. The first is planned locally and pushed down;
the second sends the SQL text verbatim via the catalog's `query` macro.

### Q8. Query all three workers

```bash
python local/quack_client.py \
  --query "worker-1=SELECT sale_status, COUNT(*) AS count_star FROM sales GROUP BY sale_status ORDER BY sale_status" \
  --query "worker-2=SELECT country, COUNT(*) AS count_star FROM customers GROUP BY country ORDER BY country" \
  --query "worker-3=SELECT category, COUNT(*) AS count_star FROM products GROUP BY category ORDER BY category"
```

**Verify** — the same row counts you saw in [H6](#h6-aggregation-across-a-group). Same data,
same SQL, different transport.

### Q9. Writes and read back

There is no `--allow-write` in Quack mode: the remote catalog handles writes natively through
DuckDB's own MVCC rather than a Python lock.

```bash
python local/quack_client.py \
  --query "worker-1=DELETE FROM sales WHERE sale_id BETWEEN 40000001 AND 40000005"
```

```bash
python local/quack_client.py \
  --query "worker-1=INSERT INTO sales (sale_id, customer_id, product_id, quantity, sales_channel, payment_method, sale_status, sale_date, sold_unit_price, discount_pct) VALUES (40000001, 1, 1, 1, 'online', 'card', 'completed', DATE '2026-09-24', 10.00, 0.00)" \
  --query "worker-1=INSERT INTO sales (sale_id, customer_id, product_id, quantity, sales_channel, payment_method, sale_status, sale_date, sold_unit_price, discount_pct) VALUES (40000002, 2, 2, 2, 'store', 'bank_transfer', 'processing', DATE '2026-09-24', 20.00, 0.00)" \
  --query "worker-1=INSERT INTO sales (sale_id, customer_id, product_id, quantity, sales_channel, payment_method, sale_status, sale_date, sold_unit_price, discount_pct) VALUES (40000003, 3, 3, 3, 'marketplace', 'wallet', 'shipped', DATE '2026-09-24', 30.00, 0.00)"
```

```bash
python local/quack_client.py \
  --query "worker-1=SELECT sale_id, sale_status, quantity FROM sales WHERE sale_id BETWEEN 40000001 AND 40000005 ORDER BY sale_id"
```

**Expected** — the three inserted rows come back. As in H7, the `DELETE` reports `0` on a fresh
seed because the 40000001–40000005 range does not exist yet.

### Q10. DDL

```bash
python local/quack_client.py \
  --query "worker-1=CREATE OR REPLACE TABLE sales_agg AS SELECT sale_status, COUNT(*) AS sale_count FROM sales GROUP BY sale_status" \
  --query "worker-2=CREATE OR REPLACE TABLE customers_agg AS SELECT country, COUNT(*) AS customer_count FROM customers GROUP BY country" \
  --query "worker-3=CREATE OR REPLACE TABLE products_agg AS SELECT category, COUNT(*) AS product_count FROM products GROUP BY category"
```

```bash
python local/quack_client.py --attach-all \
  --sql "SELECT * FROM w1.sales_agg ORDER BY sale_count DESC" \
  --sql "SELECT * FROM w2.customers_agg ORDER BY customer_count DESC" \
  --sql "SELECT * FROM w3.products_agg ORDER BY product_count DESC"
```

### Q11. Transactions are forwarded

All `--sql` arguments run on **one** persistent local connection, so a transaction can span
them. Quack forwards `BEGIN` / `COMMIT` to the server:

```bash
python local/quack_client.py --attach-all \
  --sql "BEGIN TRANSACTION" \
  --sql "INSERT INTO w1.sales (sale_id, customer_id, product_id, quantity, sales_channel, payment_method, sale_status, sale_date, sold_unit_price, discount_pct) VALUES (40000099, 9, 9, 9, 'online', 'card', 'completed', DATE '2026-09-24', 99.00, 0.00)" \
  --sql "COMMIT"
```

```bash
python local/quack_client.py --query "worker-1=SELECT COUNT(*) AS n FROM sales WHERE sale_id = 40000099"
```

**Expected** — `1`.

Send the statements separately like this rather than as one multi-statement string: the DuckDB
Python `execute()` path is built around a single statement, so a semicolon-joined block is not
reliably accepted.

To see the transaction actually roll back, replace `COMMIT` with `ROLLBACK` and use a fresh
`sale_id` — the follow-up count should then be `0`.

### Q12. Use it as a library

```bash
python - <<'PY'
import sys
sys.path.insert(0, "local")

from quack_client import QuackClient

with QuackClient() as client:
    client.attach_all()

    print(client.fetch("SELECT COUNT(*) FROM w1.sales"))
    print(client.fetch("FROM w1.query('SELECT sale_status, COUNT(*) FROM sales GROUP BY 1 ORDER BY 1')"))
    print(client.query_worker("worker-2", "SELECT country, COUNT(*) FROM customers GROUP BY 1 ORDER BY 1"))
PY
```

**Expected** — three result sets: a scalar count, a `(columns, rows)` tuple from pushed-down
SQL, and the per-country breakdown.

### Q13. Run the existing coordinator and demo from the host

Both scripts read `WORKER_N_ENDPOINT`, so the exports from step 0.4 are all that is needed.

```bash
SERVER_MODE=quack python app/related_cluster_sql.py --mode quack --wait-timeout 900 \
  --query "worker-1=SELECT 1 AS worker_1_ok" \
  --query "worker-2=SELECT 2 AS worker_2_ok" \
  --query "worker-3=SELECT 3 AS worker_3_ok"
```

**Expected** — three one-row results and a small `Start spread`.

The demo script prints the coordinator-side SQL it executes, which is the clearest proof of the
pattern:

```bash
SERVER_MODE=quack python app/quack_pattern_demo.py
```

**Expected**

```text
[query-1] worker-1: ATTACH 'quack:localhost:9491' AS remote (TYPE quack, TOKEN 'REDACTED', DISABLE_SSL true)
[query-2] worker-2: ATTACH 'quack:localhost:9492' AS remote (TYPE quack, TOKEN 'REDACTED', DISABLE_SSL true)
[query-3] worker-3: ATTACH 'quack:localhost:9493' AS remote (TYPE quack, TOKEN 'REDACTED', DISABLE_SSL true)
```

Note the `quack:` scheme and the host ports — both come from `local/.env.local`.

### Q14. Tear down

```bash
deactivate                 # leave the virtualenv
docker compose down        # keep the seeded volumes
docker compose down -v     # delete the data too
```

---

## Part 3 — Troubleshooting

### `cannot reach worker-1 at http://localhost:9491`

The workers are not in HTTP mode, or are not running.

```bash
docker compose ps
docker compose config | grep SERVER_MODE
```

In Quack mode there is no `/health` or `/query` endpoint at all — use `local/quack_client.py`
instead.

### `401` / `rejected the token`

`QUACK_TOKEN` on the host must match the one the workers were started with.

```bash
echo "$QUACK_TOKEN"
docker compose config | grep QUACK_TOKEN
```

### `could not install/load the quack extension`

- `INSTALL quack` needs outbound internet on first run; it downloads into `~/.duckdb/extensions`.
- Your host `duckdb` version must match the image. Check both:

```bash
python -c "import duckdb; print(duckdb.__version__)"
grep duckdb requirements.txt
```

### `could not attach worker-1 at quack:localhost:9491`

- Confirm the workers are in Quack mode and `Up`.
- Confirm the Quack server actually bound: `docker compose logs worker-1 | grep "Starting Quack server"`.
- Confirm the token matches and is at least 4 characters — Quack rejects shorter ones.

### `Catalog Error: Table Function with name serve does not exist!`

The worker image predates the `quack_serve` fix. Rebuild:

```bash
docker compose up -d --build worker-1 worker-2 worker-3
```

### Client tries HTTPS and the handshake fails

Workers serve plain HTTP. `quack_client.py` always passes `DISABLE_SSL true`. `localhost` is
already treated as a local URI (plain HTTP by default), but the explicit option keeps working if
you later point `WORKER_N_ENDPOINT` at a remote host.

### Writes fail with a read-only error

HTTP mode only: you forgot `--allow-write`. Quack mode has no such flag.

### `ImportError: duckdb` when running `quack_client.py`

The virtualenv is not active, or you installed into a different interpreter.

```bash
which python
pip show duckdb
```

### Lock conflict when opening the database file directly

Expected, and not worth fighting. In Quack mode `quack_server.py` holds a persistent read-write
connection, and the files live in Docker named volumes inside the VM. Always go through the
network protocol.

---

## Appendix — Cheat sheet

### One-time setup

```bash
cd ~/codebase/python-base/duckdb-local-quack-docker
docker compose build
source local/.env.local
```

### HTTP mode

```bash
docker compose up -d worker-1 worker-2 worker-3
python local/http_client.py --health
python local/http_client.py --query "worker-1=SELECT COUNT(*) AS n FROM sales"
python local/http_client.py --allow-write --query "worker-1=CREATE OR REPLACE TABLE t AS SELECT 1 AS x"
SERVER_MODE=http python app/related_cluster_sql.py --query "worker-1=SELECT 1 AS ok"
```

### Quack mode

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r local/requirements.txt

docker compose down
docker compose up -d --build worker-1 worker-2 worker-3

python local/quack_client.py --check
python local/quack_client.py --query "worker-1=SELECT COUNT(*) AS n FROM sales"
python local/quack_client.py --attach-all --tables
python local/quack_client.py --attach-all --sql "SELECT COUNT(*) FROM w1.sales"
SERVER_MODE=quack python app/related_cluster_sql.py --mode quack --query "worker-1=SELECT 1 AS ok"
```

### Client CLI flags

| Flag | `http_client.py` | `quack_client.py` |
| --- | --- | --- |
| `--query "worker-N=SQL"` | yes | yes |
| `--query-file "worker-N=PATH"` | yes | — |
| `--sql "SQL"` | — | yes (needs `--attach-all` for `w1`/`w2`/`w3`) |
| `--health` | yes | — |
| `--check` | — | yes |
| `--tables` | — | yes |
| `--attach-all` | — | yes |
| `--allow-write` | yes | not needed |
| `--token` | yes | yes |
| `--timeout` | yes | — |
| `--max-rows` | yes | yes |

Both scripts exit non-zero if any query failed, so they are safe to use in shell pipelines and
CI.
