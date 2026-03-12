# Analytics Benchmark: SQLite & PostgreSQL vs DuckDB + Parquet

A self-contained benchmark suite that pits two traditional row-store databases — **SQLite** and **PostgreSQL** — against **DuckDB + Hive-partitioned Parquet** (columnar) for ad-analytics dashboard queries.

Each benchmark generates 10 million rows of realistic ad-performance data across **100 clients** and **10 ad channels**, loads the same dataset into both engines, runs 15 benchmark queries, and writes three result files so you can inspect every number side-by-side.

---

## Benchmarks at a glance

| Script | Row store | Columnar | Output prefix |
|---|---|---|---|
| `benchmark_sqlite.py` | SQLite (embedded, file-based) | DuckDB + Parquet | `results_sqlite` |
| `benchmark_sqlite_postgresql.py` | PostgreSQL (local server) | DuckDB + Parquet | `results_postgresql` |

Both scripts are structurally identical — same generated data, same 15 queries, same output format — so results are directly comparable across all three engines.

---

## What it measures

| Category | Queries | What it proves |
|---|---|---|
| **A — Accuracy Verification** | A1, A2, A3 | All engines return identical results |
| **B — Dashboard Filters** | B1, B2, B3 | Multi-criteria filtering with date ranges |
| **C — Sorting & Ranking** | C1, C2, C3 | ORDER BY on raw and computed metrics (CTR, CPA) |
| **D — Time Series & Widgets** | D1, D2, D3 | Daily trends, monthly rollups, pie-chart distributions |
| **E — Partition & Columnar Proof** | E1, E2 | Queries designed to expose the full-scan penalty of row stores |
| **F — Row-Based Fetching** | F1 | Narrow point-lookups where row stores are competitive |

---

## How it works

```
benchmark_sqlite.py  /  benchmark_sqlite_postgresql.py
│
├── Step 1 — Generate data
│     100 clients × 10 channels × 366 days × ~28 ads/partition
│     Channel-specific cost profiles + seasonal spend multipliers
│     Fixed random seed — identical rows every run
│
├── Step 2 — Load into row store
│     SQLite:      single .db file  •  two covering indexes
│     PostgreSQL:  local server     •  same schema + two covering indexes
│     10 M rows bulk-inserted in 10 K batches
│
├── Step 3 — Write Parquet
│     Hive-partitioned by composite key k = client_id + channel_id
│     1,000 partitions  •  Snappy compression
│     Written via DuckDB's COPY … TO … (FORMAT PARQUET)
│
├── Step 4 — Run row-store queries   (median of 3 runs each)
├── Step 5 — Run DuckDB queries      (median of 3 runs each)
│
└── Step 6 — Write results
      results_<engine>.txt     raw row-store output
      results_duckdb.txt       raw DuckDB output
      results_comparison.txt   side-by-side with PASS/FAIL accuracy check
```

### Partition key `k`

Every row carries a zero-padded composite key that encodes the client and channel:

```
k = f"{client_id:03d}{channel_id:02d}"
# client 1,   channel 1  →  "00101"
# client 7,   channel 3  →  "00703"
# client 100, channel 10 →  "10010"
```

DuckDB queries filter on `k IN (...)` instead of bare `client_id` / `channel_id` predicates, so the Parquet reader can prune entire partition directories before reading a single byte of data.

---

## Requirements

| Dependency | Purpose | SQLite script | PostgreSQL script |
|---|---|:---:|:---:|
| Python ≥ 3.13 | Runtime | ✓ | ✓ |
| `duckdb >= 1.5.0` | Columnar query engine + Parquet writer | ✓ | ✓ |
| `tabulate >= 0.10.0` | Pretty-print result tables | ✓ | ✓ |
| `psycopg[binary] >= 3.2.0` | PostgreSQL driver (bundles libpq) | — | ✓ |
| SQLite | Embedded in Python stdlib | ✓ | — |
| PostgreSQL server | Local instance (any recent version) | — | ✓ |

Install Python dependencies with **uv** (recommended):

```sh
uv sync
```

Or with pip:

```sh
pip install duckdb tabulate "psycopg[binary]"
```

---

## Running the SQLite benchmark

```sh
# Full run — 10 million rows (takes ~5–10 min depending on hardware)
uv run python benchmark_sqlite.py

# Quick smoke-test — 100 K rows (completes in ~10 s)
NUM_ROWS=100000 uv run python benchmark_sqlite.py

# Keep the SQLite database and Parquet files after the run
SKIP_CLEANUP=1 uv run python benchmark_sqlite.py
```

### Environment variables — SQLite

| Variable | Default | Description |
|---|---|---|
| `NUM_ROWS` | `10_000_000` | Total rows to generate |
| `SKIP_CLEANUP` | `0` | Set to `1` to retain `data/` after the run |

---

## Running the PostgreSQL benchmark

### Prerequisites

1. A running local PostgreSQL instance.
2. A database that the configured user can connect to and create tables in.
   The default database name is `benchmark_poc_db` — create it if it does not exist:

```sql
CREATE DATABASE benchmark_poc_db;
```

3. The script drops and recreates the `ad_insights` table on every run, so no manual schema setup is needed.

```sh
# Full run with defaults (localhost:5432, user postgres, db benchmark_poc_db)
uv run python benchmark_sqlite_postgresql.py

# Quick smoke-test
NUM_ROWS=100000 uv run python benchmark_sqlite_postgresql.py

# Custom connection
PG_HOST=myhost PG_PORT=5432 PG_USER=analyst PG_PASSWORD=secret \
  PG_DBNAME=benchmark_poc_db uv run python benchmark_sqlite_postgresql.py

# Keep the table and Parquet files after the run
SKIP_CLEANUP=1 uv run python benchmark_sqlite_postgresql.py
```

### Environment variables — PostgreSQL

| Variable | Default | Description |
|---|---|---|
| `PG_HOST` | `localhost` | PostgreSQL host |
| `PG_PORT` | `5432` | PostgreSQL port |
| `PG_USER` | `postgres` | Database user |
| `PG_PASSWORD` | `postgres` | Database password |
| `PG_DBNAME` | `benchmark_poc_db` | Database name |
| `NUM_ROWS` | `10_000_000` | Total rows to generate |
| `SKIP_CLEANUP` | `0` | Set to `1` to retain table and `data/` after the run |

---

## Output files

Both scripts produce the same three output files (with different engine-name prefixes):

| File | Contents |
|---|---|
| `results_sqlite.txt` / `results_postgresql.txt` | Every query result as returned by the row-store engine, with timing |
| `results_duckdb.txt` | Every query result as returned by DuckDB, with timing |
| `results_comparison.txt` | Performance summary table + side-by-side row-level comparison with PASS/FAIL accuracy check for each query |

---

## Why DuckDB + Parquet wins on analytics

### Hive partition pruning

The 10 M rows are split across **1,000 Parquet directories** (`k=00101/`, `k=00102/`, …). A query for a single client touches at most 10 directories (one per channel) and skips the other 990 entirely — a 99 % reduction in files opened before any data is decoded.

Query E1 deliberately omits `client_id` so only `channel_id = 2` is filtered. SQLite and PostgreSQL must fall back to a full table scan because neither composite index starts with `channel_id`. DuckDB uses the `k IN (...)` clause to open only the 100 `channel 2` partitions — 90 % of directories skipped at the filesystem level.

### Columnar storage

Parquet stores each column in a separate byte range within the file. A query that aggregates `impressions`, `clicks`, `spend`, and `conversions` reads **4 of 13 columns** — roughly 70 % fewer bytes transferred from disk. All reads are sequential, so OS read-ahead works perfectly. Row stores must read every column of every matched row even when most columns are irrelevant.

### Vectorised execution

DuckDB processes data in batches of thousands of values per CPU cycle using SIMD instructions. Both SQLite and PostgreSQL use traditional row-at-a-time (Volcano model) execution — one tuple flows through the operator tree per cycle.

### Snappy compression

Individual Parquet column chunks compress far better than mixed row data. Numeric columns like `impressions` and `clicks` achieve especially high compression ratios, further reducing I/O.

### Where row stores win

For **narrow point-lookups** (query F1: fetch all rows for one client in one month) both row stores deliver very fast response times via their B-tree indexes. PostgreSQL additionally benefits from its shared buffer cache — repeated identical queries can be served entirely from RAM.

At small dataset sizes (< ~500 K rows) both row stores' lower per-query startup overhead can keep them competitive or faster on every category, since DuckDB's first-query JIT compilation cost is not yet amortised.

---

## Data model

```
ad_insights
├── id              BIGINT PRIMARY KEY
├── client_id       INTEGER            -- 1–100
├── channel_id      INTEGER            -- 1–10
├── ad_account_id   VARCHAR(64)
├── campaign_id     VARCHAR(128)
├── campaign_name   VARCHAR(256)
├── ad_id           VARCHAR(128)
├── ad_name         VARCHAR(256)
├── impressions     BIGINT
├── clicks          BIGINT
├── spend           DOUBLE / NUMERIC(14,2)   -- DOUBLE in SQLite, NUMERIC in PG
├── conversions     INTEGER
└── date            DATE
```

Indexes (both engines):

```sql
CREATE INDEX idx_client_channel_date ON ad_insights (client_id, channel_id, date);
CREATE INDEX idx_client_date         ON ad_insights (client_id, date);
```

Parquet partition layout:

```
data/insights/
  k=00101/   ← client 1,   channel 1
  k=00102/   ← client 1,   channel 2
  ...
  k=10010/   ← client 100, channel 10
```

---

## SQL dialect differences

The PostgreSQL queries differ from their SQLite counterparts in two places:

| Operation | SQLite | PostgreSQL |
|---|---|---|
| Extract month from date | `CAST(strftime('%m', date) AS INTEGER)` | `DATE_PART('month', date)::INTEGER` |
| Round a computed float | `ROUND(SUM(spend), 2)` | `ROUND(SUM(spend)::NUMERIC, 2)` |
| Round a division result | `ROUND(a / NULLIF(b, 0), 4)` | `ROUND((a / NULLIF(b, 0))::NUMERIC, 4)` |

PostgreSQL's `ROUND` requires an explicit `NUMERIC` cast when operating on a `DOUBLE PRECISION` or division expression; SQLite accepts bare floats. The `::NUMERIC` casts are applied only to the PostgreSQL (`"pg"`) query variants — DuckDB queries are unchanged.

---

## Realistic data characteristics

- **100 clients** — generated from 20 prefixes × 25 industries × 5 suffixes (e.g. *Apex Retail Corp*, *Bolt Technology Co*)
- **10 ad channels** — Facebook, Google, TikTok, LinkedIn, Snapchat, Pinterest, YouTube, X (Twitter), Reddit, Amazon Ads
- **Channel profiles** — each platform has its own impressions range, CPC range, and baseline conversion rate reflecting real-world cost structures
- **Seasonal multipliers** — spend peaks in November (+40 %) and December (+30 %), troughs in January (–20 %) to simulate real ad seasonality
- **366 days** — full leap year 2024-01-01 → 2024-12-31
- **Fixed seed** — `RANDOM_SEED = 42` ensures every run produces exactly the same dataset

---

## Project structure

```
.
├── benchmark_sqlite.py              ← SQLite vs DuckDB + Parquet
├── benchmark_sqlite_postgresql.py   ← PostgreSQL vs DuckDB + Parquet
├── benchmark.py                     ← original MySQL vs DuckDB + Parquet
├── pyproject.toml                   ← dependencies (duckdb, psycopg, tabulate)
├── uv.lock
│
├── results_sqlite.txt               ← generated by benchmark_sqlite.py
├── results_postgresql.txt           ← generated by benchmark_sqlite_postgresql.py
├── results_duckdb.txt               ← generated by either script
├── results_comparison.txt           ← generated by either script
│
└── data/                            ← generated on run, removed by cleanup
    ├── benchmark_poc_db.db          ← SQLite database (benchmark_sqlite.py only)
    └── insights/                    ← Hive-partitioned Parquet tree (both scripts)
        ├── k=00101/
        │   └── *.parquet
        ├── k=00102/
        │   └── *.parquet
        └── ...
```
