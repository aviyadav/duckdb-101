# duckdb-secrets

Query Parquet files on an S3-compatible object store ([RustFS](https://rustfs.com)) using
[DuckDB](https://duckdb.org) secrets and the built-in `httpfs` extension — no boto3, no
credential plumbing.

## Requirements

- Python >= 3.14 with [uv](https://docs.astral.sh/uv/)
- A RustFS (MinIO-compatible) server at `localhost:9000` (`http://localhost:9001/rustfs/console` for web console)

## Setup

```sh
uv sync
```

---

## 1. Scoped Persistent Secrets Example

Implements the multi-bucket persistent scoped secrets architecture from `Scoped-Persistent-Secrets.docx`.

### Overview

When using `SCOPE`, DuckDB builds a routing table for external connections. When querying
`s3://company-analytics-bucket/...` or `s3://company-logs-bucket/...`, DuckDB automatically
matches the URI prefix against your registered scopes to apply the correct credentials and
endpoints without collisions.

Because these secrets are `PERSISTENT`, they are stored locally in `~/.duckdb/stored_secrets/`
as binary files (`analytics_s3.duckdb_secret` and `logs_s3.duckdb_secret`). Any new DuckDB
connection automatically loads them without needing any hardcoded keys in application code.

### Step 1: Create Buckets, Seed Parquet Files & Register Persistent Secrets

Run the setup command:

```sh
uv run setup-scoped
```

This automates:
1. Creating `company-analytics-bucket` and `company-logs-bucket` on RustFS via S3 API.
2. Generating sample datasets:
   - `s3://company-analytics-bucket/users.parquet` (`user_id`, `user_name`)
   - `s3://company-logs-bucket/2026/events.parquet` (`user_id`, `event_type`, `event_time`)
3. Creating persistent secrets in DuckDB:

```sql
-- Secret A: Dedicated to the analytics bucket
CREATE PERSISTENT SECRET analytics_s3 (
    TYPE S3,
    KEY_ID 'rustfsadmin',
    SECRET 'rustfsadmin',
    ENDPOINT 'localhost:9000/company-analytics-bucket',
    REGION 'us-east-1',
    URL_STYLE 'path-style',
    USE_SSL false,
    SCOPE 's3://company-analytics-bucket/'
);

-- Secret B: Dedicated to raw log storage
CREATE PERSISTENT SECRET logs_s3 (
    TYPE S3,
    KEY_ID 'rustfsadmin',
    SECRET 'rustfsadmin',
    ENDPOINT 'localhost:9000/company-logs-bucket',
    REGION 'us-east-1',
    URL_STYLE 'path-style',
    USE_SSL false,
    SCOPE 's3://company-logs-bucket/'
);
```

### Step 2: Query Across Buckets in a Single SQL Statement

Run:

```sh
uv run query-scoped
```

The script in [`src/duckdb_secrets/scoped_persistent_secrets.py`](src/duckdb_secrets/scoped_persistent_secrets.py)
contains **no credentials or secret definitions**:

```python
import duckdb

# Connect to DuckDB (automatically loads persistent secrets from ~/.duckdb/stored_secrets/)
con = duckdb.connect()

# Query directly across both buckets—DuckDB matches the S3 paths to your saved scopes
query = """
    SELECT 
        a.user_id,
        a.user_name,
        l.event_type,
        l.event_time
    FROM read_parquet('s3://company-analytics-bucket/users.parquet') AS a
    JOIN read_parquet('s3://company-logs-bucket/2026/events.parquet') AS l
      ON a.user_id = l.user_id
    LIMIT 10;
"""
df = con.execute(query).df()
print(df)
```

Output:

```
   user_id      user_name event_type          event_time
0        1    Alice Smith      login 2026-01-15 08:30:00
1        1    Alice Smith   purchase 2026-01-15 08:45:00
2        2      Bob Jones      login 2026-01-15 09:12:00
3        2      Bob Jones     logout 2026-01-15 09:30:00
4        3  Charlie Brown      login 2026-01-16 10:05:00
5        3  Charlie Brown     search 2026-01-16 10:10:00
6        4   Diana Prince      login 2026-01-16 11:00:00
7        4   Diana Prince   click_ad 2026-01-16 11:05:00
8        5    Evan Wright      login 2026-01-17 14:20:00
9        5    Evan Wright   checkout 2026-01-17 14:35:00
```

---

## 2. In-Memory Secret Query Example

```sh
uv run query-bucket
```

Reads the latest orders from the `warehouse` bucket and prints them:

```
┌─────────────────────────┬───────────────────┬─────────────────────┬───┬─────────┬──────────────────────┬─────────────┐
│        order_id         │  customer_email   │       product       │ … │ status  │      created_at      │ created_day │
│         varchar         │      varchar      │       varchar       │ … │ varchar │ timestamp with time… │    date     │
├─────────────────────────┼───────────────────┼─────────────────────┼───┼─────────┼──────────────────────┼─────────────┤
│ 9599d764-0fec-4c5d-b3b… │ alice@example.com │ Mechanical Keyboard │ … │ SHIPPED │ 2026-08-11 22:57:40… │ 2026-08-11  │
│ a1182f08-d748-4003-a3d… │ bob@example.com   │ USB-C Hub           │ … │ PENDING │ 2026-08-11 23:04:35… │ 2026-08-11  │
│ 9ed0fa05-daa1-411d-87d… │ carol@example.com │ 27" Monitor         │ … │ PENDING │ 2026-08-11 23:04:35… │ 2026-08-11  │
└─────────────────────────┴───────────────────┴─────────────────────┴───┴─────────┴──────────────────────┴─────────────┘
```

The query lives in [`src/duckdb_secrets/query_my_bucket.py`](src/duckdb_secrets/query_my_bucket.py):

```sql
SELECT * FROM read_parquet('s3://warehouse/shop/orders/data/**/*.parquet') LIMIT 5;
```

---

## Connection & Path-Style Configuration

The local RustFS server runs S3 API on port 9000 (with web console on `http://localhost:9001/rustfs/console`).

### DuckDB 1.5.x path-style quirk

With `URL_STYLE 'path-style'`, DuckDB 1.5.x omits the bucket from S3 requests when targeting custom endpoints unless the bucket is included in the `ENDPOINT` parameter (`ENDPOINT 'host:port/bucket'`).

By combining `ENDPOINT 'localhost:9000/<bucket>'` with `SCOPE 's3://<bucket>/'`, DuckDB:
1. Matches any query to `s3://<bucket>/...` using the `SCOPE`.
2. Directs the request to the correct bucket endpoint on RustFS.
3. Allows joining across multiple buckets in a single query seamlessly.
