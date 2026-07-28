# Partitioned synthetic AWS VPC Flow Logs

Generates synthetic flow-log data from **2024-01-01 through 2026-07-16**, inclusive. Although the request described CloudTrail logs, the columns in the supplied query (`src`, `dst`, `sp`, `dp`, `pkt`, and `byt`) are the abbreviated AWS VPC Flow Log schema, so this generator uses that format.

## Generate data

```sh
uv run main.py
```

By default, this creates 1,000 records per day (928,000 total) in 31 monthly, Hive-style partitions:

```text
base_flow/
├── year=2024/
│   ├── month=01/flow_logs.parquet
│   └── ...
└── year=2026/
    └── month=07/flow_logs.parquet
```

Choose a smaller or larger dataset with `--rows-per-day`, and use `--seed` for a different deterministic sample:

```sh
uv run main.py --rows-per-day 100 --seed 7 --output base_flow
```

Each source column is stored as text to model raw AWS logs. About 2% of records have a `NODATA` or `SKIPDATA` status and use `-` for unavailable fields, allowing `TRY_CAST` to convert those values to `NULL`.

Records are generated and written in batches of at most 10,000. The program never accumulates a day or month of data in memory, so increasing `--rows-per-day` increases disk usage and runtime without making memory usage grow proportionally.

## Query with DuckDB

Open a persistent DuckDB database from the project directory:

```sh
duckdb analytics.db
```

The filename matters: running only `duckdb` creates an in-memory database whose tables disappear when the session ends.

The source data can be queried directly with:

```sql
SELECT
    "year",
    "month",
    v, acc, id, src, dst,
    TRY_CAST(sp AS INTEGER) AS sp,
    TRY_CAST(dp AS INTEGER) AS dp,
    pr,
    TRY_CAST(pkt AS BIGINT) AS pkt,
    -- TRY_CAST turns the AWS '-' sentinel into NULL.
    TRY_CAST(byt AS BIGINT) AS byt,
    TRY_CAST("start" AS BIGINT) AS "start",
    TRY_CAST("end" AS BIGINT) AS "end",
    act, st
FROM read_parquet(
    'base_flow/year=*/month=*/*.parquet',
    hive_partitioning = true,
    union_by_name = true -- Match columns by name across files.
);
```

DuckDB comments begin with `--`. A single `-` is parsed as the subtraction operator and causes a parser error when used as a comment.

`union_by_name = true` handles Parquet files whose columns differ or appear in a different order. `hive_partitioning = true` derives the `year` and `month` columns from the directory names.

## Parquet view versus persistent table

### Create a view over Parquet

A view stores only the query definition. It does **not** import or cache the rows, so every query reads the relevant Parquet files and performs the casts again.

```sql
CREATE OR REPLACE VIEW flow_logs_parquet AS
SELECT
    "year",
    "month",
    v, acc, id, src, dst,
    TRY_CAST(sp AS INTEGER) AS sp,
    TRY_CAST(dp AS INTEGER) AS dp,
    pr,
    TRY_CAST(pkt AS BIGINT) AS pkt,
    TRY_CAST(byt AS BIGINT) AS byt,
    TRY_CAST("start" AS BIGINT) AS "start",
    TRY_CAST("end" AS BIGINT) AS "end",
    act, st
FROM read_parquet(
    'base_flow/year=*/month=*/*.parquet',
    hive_partitioning = true,
    union_by_name = true
);
```

This is convenient when Parquet is the source of truth and new files must become visible without reloading a table.

### Materialize a persistent table

Import the normalized result into DuckDB once:

```sql
CREATE OR REPLACE TABLE flow_logs AS
SELECT * FROM flow_logs_parquet;

CHECKPOINT;
```

Because the CLI was opened with `duckdb analytics.db`, `flow_logs` is stored in `analytics.db` and remains available in later sessions.

### Why persistence is usually faster

For repeated analytical queries, the persistent table is typically faster because DuckDB can:

- read its native columnar storage instead of opening and coordinating many external files;
- store `sp`, `dp`, `pkt`, `byt`, `start`, and `end` as numeric columns, avoiding repeated `TRY_CAST` work;
- use its own compression, row-group metadata, zone maps, and statistics;
- avoid repeatedly reconciling schemas and discovering Hive partitions;
- cache frequently accessed database pages effectively across repeated queries.

Persistence has an up-front import cost and duplicates the source data. It is not guaranteed to win every query: direct Parquet can be preferable for one-off queries, data-lake workflows, or queries that prune almost all files using `year` and `month`. A view also reflects newly added Parquet files immediately, whereas a materialized table must be refreshed.

## Compare performance

Enable timing in the DuckDB CLI and run equivalent queries more than once to reduce cold-cache effects:

```sql
.timer on

SELECT "year", "month", count(*), sum(byt)
FROM flow_logs_parquet
GROUP BY "year", "month"
ORDER BY "year", "month";

SELECT "year", "month", count(*), sum(byt)
FROM flow_logs
GROUP BY "year", "month"
ORDER BY "year", "month";
```

For a more rigorous comparison, use `EXPLAIN ANALYZE` and run each query several times in alternating order:

```sql
EXPLAIN ANALYZE
SELECT count(*), sum(pkt), sum(byt)
FROM flow_logs_parquet
WHERE "year" = 2025;

EXPLAIN ANALYZE
SELECT count(*), sum(pkt), sum(byt)
FROM flow_logs
WHERE "year" = 2025;
```

## Refresh the persistent table

After regenerating or adding Parquet files, rebuild the table so it matches the view:

```sql
CREATE OR REPLACE TABLE flow_logs AS
SELECT * FROM flow_logs_parquet;

CHECKPOINT;
```

For append-only ingestion, insert only a known new partition instead of rebuilding the entire table, and ensure that the same partition is not loaded twice.
