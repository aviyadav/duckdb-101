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

Demonstrates writing to and reading from a production-style Iceberg table: initial load, incremental upsert with `MERGE INTO`, and time-travel queries.

### Infrastructure — Iceberg on Docker

Setup B requires two services running locally:

| Service | Role | Default address |
|---|---|---|
| **MinIO** | S3-compatible object storage (stores Iceberg data files) | `http://127.0.0.1:9000` |
| **Iceberg REST catalog** | Tracks table metadata and snapshots | `http://127.0.0.1:8181` |

#### `docker-compose.yml`

Create this file in the project root (or anywhere convenient) and run it before the setup_b scripts:

```yaml
services:
  minio:
    image: minio/minio:latest
    container_name: minio
    ports:
      - "9000:9000"   # S3 API
      - "9001:9001"   # MinIO web console
    environment:
      MINIO_ROOT_USER: admin
      MINIO_ROOT_PASSWORD: password
    command: server /data --console-address ":9001"
    volumes:
      - minio-data:/data
    healthcheck:
      test: ["CMD", "mc", "ready", "local"]
      interval: 5s
      timeout: 5s
      retries: 5

  minio-init:
    image: minio/mc:latest
    container_name: minio-init
    depends_on:
      minio:
        condition: service_healthy
    entrypoint: >
      /bin/sh -c "
        mc alias set local http://minio:9000 admin password &&
        mc mb local/warehouse --ignore-existing &&
        echo 'Bucket ready'
      "

  iceberg-rest:
    image: tabulario/iceberg-rest:latest
    container_name: iceberg-rest
    depends_on:
      - minio-init
    ports:
      - "8181:8181"
    environment:
      CATALOG_WAREHOUSE: s3://warehouse/
      CATALOG_IO__IMPL: org.apache.iceberg.aws.s3.S3FileIO
      CATALOG_S3_ENDPOINT: http://minio:9000
      CATALOG_S3_ACCESS__KEY__ID: admin
      CATALOG_S3_SECRET__ACCESS__KEY: password
      CATALOG_S3_PATH__STYLE__ACCESS: "true"

volumes:
  minio-data:
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
