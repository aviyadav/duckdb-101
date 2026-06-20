# DuckDB Pipeline

A two-part data engineering lab that demonstrates DuckDB as an analytics engine — first against local Parquet files, then against a full Iceberg lakehouse backed by MinIO and an Iceberg REST catalog.

---

## Project structure

```
duckdb-pipeline/
├── data/
│   ├── raw/
│   │   ├── sales.parquet               # base dataset (2 M rows, setup_a)
│   │   └── sales_incremental.parquet   # incremental batch (100 K rows, setup_b)
│   └── processed/
│       └── sales_summary.parquet       # pipeline_local.py output
├── setup_a/
│   ├── generate_data.py                # generate the 2 M-row base dataset
│   ├── pipeline_local.py               # aggregate & write to analytics.duckdb
│   └── inspect_plan.py                 # EXPLAIN ANALYZE a query
├── setup_b/
│   ├── generate_incremental.py         # generate a 100 K-row incremental batch
│   ├── pipeline_iceberg.py             # initial load into the Iceberg table
│   ├── merge_iceberg.py                # incremental upsert via MERGE INTO
│   └── timetravel_iceberg.py           # query historical Iceberg snapshots
├── pyproject.toml
└── README.md
```

---

## Prerequisites

| Tool | Version |
|---|---|
| Python | ≥ 3.14 |
| uv | any recent |
| Docker + Docker Compose | for setup_b |

Install Python dependencies:

```bash
uv sync
```

---

## Setup A — Local DuckDB pipeline

Demonstrates generating synthetic sales data, running an aggregation pipeline entirely within DuckDB, and inspecting its query plan. No external services required.

### Data schema

`data/raw/sales.parquet`

| Column | Type | Description |
|---|---|---|
| `order_id` | INTEGER | Random order identifier (0–1 000 000) |
| `category` | VARCHAR | `electronics`, `clothing`, `food`, `books` |
| `country` | VARCHAR | `US`, `DE`, `FR`, `BR`, `JP` |
| `amount` | DECIMAL(10,2) | Order value $5–$505 |
| `discount_rate` | DECIMAL(5,4) | Discount 0–30 % |
| `order_date` | TIMESTAMP | Random date within the past 365 days |

### Step 1 — Generate base data (2 M rows)

```bash
uv run setup_a/generate_data.py
```

Writes `data/raw/sales.parquet` (ZSTD-compressed Parquet).

### Step 2 — Run the local pipeline

```bash
uv run setup_a/pipeline_local.py
```

Reads `sales.parquet`, builds a monthly aggregation with a per-category revenue rank, writes the result to `data/processed/sales_summary.parquet` and `analytics.=.duckdb`, then prints a category-level revenue summary.

### Step 3 — Inspect the query plan (optional)

```bash
uv run setup_a/inspect_plan.py
```

Runs `EXPLAIN ANALYZE` on a filtered aggregation and prints the physical plan. Useful for understanding DuckDB's predicate-pushdown and parallelism.

---

## Setup B — Iceberg lakehouse pipeline

This is the argument that changes the scope of what DuckDB is. Since v1.4.0 LTS in September 2025, DuckDB writes to Apache Iceberg tables with full ACID semantics. Since v1.5.3 in May 2026, it supports MERGE INTO upserts against Iceberg tables connected to REST catalogs. The table you write is immediately readable by Spark, Trino, or Flink. The format is the contract, not the engine.

Start The Local Catalog (Lab Setup)

```
# Clone the DuckDB Iceberg test scripts
git clone https://github.com/duckdb/duckdb-iceberg.git
cd duckdb-iceberg

# Start a local REST catalog on port 8181 and MinIO on port 9000
docker compose -f scripts/docker-compose.yml up -d
```


#### Start the stack

```bash
docker compose up -d
```

Wait for the `iceberg-rest` container to be healthy (a few seconds), then verify:

```bash
# MinIO console
open http://localhost:9001          # login: admin / password

# Iceberg catalog health
curl http://localhost:8181/v1/config
```

#### Stop and clean up

```bash
docker compose down          # stop containers, keep volume
docker compose down -v       # stop containers and delete stored data
```

---

### Step 1 — Generate incremental data (100 K rows)

```bash
uv run setup_b/generate_incremental.py
```

Writes `data/raw/sales_incremental.parquet`. The batch is split into two windows to exercise both `MERGE INTO` paths:

| Rows | `order_date` window | MERGE outcome |
|---|---|---|
| 90 000 | Last 7 days | `WHEN MATCHED → UPDATE` existing monthly aggregates |
| 10 000 | 32–62 days in the future | `WHEN NOT MATCHED → INSERT` new monthly rows |

### Step 2 — Initial load into Iceberg

```bash
uv run setup_b/pipeline_iceberg.py
```

Creates schema `lakehouse.analytics` and table `sales_summary`, then inserts the full aggregation of `sales.parquet` as the first Iceberg snapshot.

`sales_summary` schema:

| Column | Type |
|---|---|
| `category` | VARCHAR |
| `country` | VARCHAR |
| `month` | TIMESTAMP |
| `total_orders` | BIGINT |
| `net_revenue` | DECIMAL(18,2) |

### Step 3 — Incremental upsert

```bash
uv run setup_b/merge_iceberg.py
```

Aggregates `sales_incremental.parquet` into a temp table, then runs `MERGE INTO` against `lakehouse.analytics.sales_summary`:

- **Matched** rows (same `category + country + month`): adds new order counts and revenue to existing totals.
- **Unmatched** rows (new month combinations): inserts them as fresh rows.

This creates a second Iceberg snapshot, preserving the first for time-travel.

### Step 4 — Time-travel query

```bash
uv run setup_b/timetravel_iceberg.py
```

Lists all snapshots of `sales_summary`, then queries the table **as it existed before the upsert** (snapshot 1) using the Iceberg `AT (VERSION => <snapshot_id>)` syntax.

---

## How the pieces fit together

```
setup_a/generate_data.py
        │
        ▼
data/raw/sales.parquet ──────────────────────────────┐
        │                                            │
        ▼                                            ▼
setup_a/pipeline_local.py              setup_b/pipeline_iceberg.py
        │                                  (initial load → snapshot 1)
        ▼                                            │
data/processed/sales_summary.parquet                 ▼
analytics.=.duckdb                   setup_b/generate_incremental.py
                                                     │
                                                     ▼
                                     data/raw/sales_incremental.parquet
                                                     │
                                                     ▼
                                       setup_b/merge_iceberg.py
                                      (upsert → snapshot 2)
                                                     │
                                                     ▼
                                     setup_b/timetravel_iceberg.py
                                     (query snapshot 1 vs snapshot 2)
```
