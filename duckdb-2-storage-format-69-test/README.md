# DuckDB 2.0 Storage Format v69 Benchmark

This project evaluates DuckDB 2.0 and its native **v69 storage format** against
DuckDB 1.5.6 and the older **v68 storage format**. It uses a generated NetFlow-style
dataset to compare database file size, connection startup, schema inspection, row
counting, and a monthly analytical aggregation.

The original expectations were that DuckDB 2.0 and storage format v69 would:

1. reduce the database's on-disk size; and
2. improve analytical query performance.

The experiment is useful because it separates two changes that are easy to conflate:
the **DuckDB engine version** and the **database file's storage version**.

## Executive summary

- The files were verified as genuine v68 and v69 databases. DuckDB reports storage
  tags `v1.5.0+` for `analytics.db` and `v2.0.0+` for `analytics_v69.db`.
- The v69 file is **4.5 MiB smaller**, a reduction of only **0.257%**. Both files
  therefore round to 1.7 GiB. Size is effectively unchanged, but not byte-for-byte
  identical.
- The saved timing logs do **not** show identical performance. In their single runs,
  DuckDB 2.0.0 on v68 was fastest for the monthly aggregation (0.1063 seconds),
  followed by DuckDB 2.0.0 on v69 (0.2442 seconds), then DuckDB 1.5.6 on v68
  (0.6721 seconds).
- Those timings are not sufficient to declare a general DuckDB 2.0 or v69 speedup.
  Each case was run only once, in a fixed order, without controlling the operating
  system page cache. The v69 result in particular should not be interpreted as proof
  that v69 is slower.
- `PRAGMA storage_info('flow_logs')` reports the **same compression methods and
  segment counts for every column** in v68 and v69. This directly explains most of
  the size similarity and why this particular query sees little format-related
  difference.
- The operation labelled “Full Table Scan” is only `SELECT count(*)`. DuckDB can
  answer it from table metadata in about a millisecond; it is not a meaningful full
  table I/O benchmark.

## Experiment matrix

The three cases isolate the engine and storage-format changes:

| Case | DuckDB engine | Database file | Storage format | What it tests |
|---|---:|---|---|---|
| Baseline | 1.5.6 | `analytics.db` | v68 (`v1.5.0+`) | DuckDB 1.5 baseline |
| Engine upgrade only | 2.0.0 | `analytics.db` | v68 (`v1.5.0+`) | Engine effect while holding storage format constant |
| Engine and format upgrade | 2.0.0 | `analytics_v69.db` | v69 (`v2.0.0+`) | Native DuckDB 2.0 file |

The clean comparisons are:

- **1.5.6/v68 versus 2.0.0/v68:** estimates the engine-version effect.
- **2.0.0/v68 versus 2.0.0/v69:** estimates the storage-format effect.
- **1.5.6/v68 versus 2.0.0/v69:** measures the combined migration, but cannot by
  itself identify which change caused a difference.

Opening a v68 database with DuckDB 2.0 does not turn it into a v69 database. The
explicit conversion step is required to create the native v69 file.

## Repository layout

| Path | Purpose |
|---|---|
| `generate_flow_logs.py` | Generates `analytics.db` and its `flow_logs` table |
| `convert_to_v69.py` | Copies the v68 database into a new native v69 database |
| `benchmark_v15.py` | Benchmark for DuckDB 1.5.6 on `analytics.db` |
| `benchmark_v2.py` | Benchmark for DuckDB 2.0.0 on the same v68 file |
| `benchmark_v2_v69.py` | Benchmark for DuckDB 2.0.0 on `analytics_v69.db` |
| `analytics.db` | Generated 50-million-row v68 database |
| `analytics_v69.db` | Converted 50-million-row v69 database |
| `duckdb-1.5.6-v68.log` | Saved baseline timing output |
| `duckdb-2.0.0-v68.log` | Saved engine-only upgrade timing output |
| `duckdb-2.0.0-v69.log` | Saved native v69 timing output |
| `pyproject.toml` / `uv.lock` | Python project and locked dependencies |

The benchmark scripts return query results through **Polars** using DuckDB's `.pl()`
method. pandas is not required by the current benchmark code.

## Dataset

The generator creates one table, `flow_logs`, containing **50,000,000 rows across 36
calendar months** by default. Rows are generated inside DuckDB in batches of one
million and ordered by timestamp within each batch. The default random seed is 0.42;
exact reproducibility requires `--threads 1`.

| Column | Type | Description |
|---|---|---|
| `flow_id` | `BIGINT` | Sequential flow identifier |
| `ts` | `TIMESTAMP` | Random flow start time within its assigned month |
| `year` | `SMALLINT` | Year extracted from `ts` |
| `month` | `UTINYINT` | Month extracted from `ts` |
| `src_ip` | `VARCHAR` | Internal `10.x.x.x` source address |
| `dst_ip` | `VARCHAR` | Mostly public-looking destination addresses |
| `src_port` | `INTEGER` | Ephemeral source port |
| `dst_port` | `INTEGER` | Weighted common service port |
| `proto` | `VARCHAR` | Approximately 82% TCP, 15% UDP, and 3% ICMP |
| `packets` | `BIGINT` | Packet count derived from flow size |
| `byt` | `BIGINT` | Flow bytes, using a heavy-tailed distribution |
| `duration_ms` | `INTEGER` | Log-uniform flow duration |

The monthly benchmark query reads only `year`, `month`, and `byt`:

```sql
SELECT
    year,
    month,
    round(SUM(byt) / 1024.0 / 1024.0 / 1024.0, 2) AS total_gb
FROM flow_logs
GROUP BY year, month
ORDER BY year DESC, month DESC;
```

It returns 36 rows. Despite the column name `total_gb`, the divisor computes GiB
(1024³ bytes), not decimal GB (1000³ bytes).

## Prerequisites

- `uv` for running the pinned Python environments
- Python satisfying `pyproject.toml` (`>=3.15`)
- DuckDB 1.5.6 and 2.0.0, selected through `uv run --with`
- Polars for benchmark result materialization
- Enough disk space for both databases and temporary conversion files; the two main
  files alone occupy approximately 3.42 GiB

Run all commands from this project's root directory so that the relative database
paths and log filenames resolve correctly.

## Reproduction workflow

### 1. Generate the v68 database

```bash
uv run --with 'duckdb==1.5.6' generate_flow_logs.py
```

With DuckDB 1.5.6, the generator's automatic storage-version selection resolves to
`v1.5.0`, which is public storage format v68. It finishes with `CHECKPOINT` and a
clean connection close so that the benchmarks can open the file read-only without a
WAL.

The generator refuses to overwrite an existing `analytics.db` unless `--force` is
provided. For a smaller smoke test, use an explicit output path rather than replacing
the benchmark database:

```bash
uv run --with 'duckdb==1.5.6' generate_flow_logs.py \
    --db analytics_small.db --rows 5000000 --force
```

### 2. Convert v68 to native v69

```bash
uv run --with 'duckdb==2.0.0' convert_to_v69.py
```

The conversion script:

1. opens an in-memory DuckDB 2.0 connection;
2. sets an 8 GB memory limit, four threads, and `data/duckdb_tmp` as the temporary
   directory;
3. attaches `analytics.db` read-only;
4. attaches `analytics_v69.db` with `STORAGE_VERSION 'latest'`, which means v69 under
   DuckDB 2.0.0;
5. executes `COPY FROM DATABASE old_db TO new_db`;
6. verifies the destination row count and reports both file sizes.

**Warning:** the script deletes an existing `analytics_v69.db` before conversion. The
source `analytics.db` is attached read-only and is not modified.

### 3. Run the three benchmarks

Baseline: DuckDB 1.5.6 on v68:

```bash
{ time uv run --with 'duckdb==1.5.6' benchmark_v15.py; } 2>&1 | tee duckdb-1.5.6-v68.log
```

Engine upgrade only: DuckDB 2.0.0 on the same v68 file:

```bash
{ time uv run --with 'duckdb==2.0.0' benchmark_v2.py; } 2>&1 | tee duckdb-2.0.0-v68.log
```

Native format: DuckDB 2.0.0 on v69:

```bash
{ time uv run --with 'duckdb==2.0.0' benchmark_v2_v69.py; } 2>&1 | tee duckdb-2.0.0-v69.log
```

Each script measures connection creation, `SHOW TABLES`, `COUNT(*)`, and the monthly
aggregation. The aggregation timer includes query execution, result materialization,
and conversion of the 36-row result to Polars. The shell's `real` time additionally
includes Python startup and `uv` environment work, so it is not pure query time.

## Recorded results

### On-disk size

The following sizes were measured directly from the two files and cross-checked with
`PRAGMA database_size`:

| Metric | v68: `analytics.db` | v69: `analytics_v69.db` |
|---|---:|---:|
| Exact size | 1,836,068,864 bytes | 1,831,350,272 bytes |
| Binary size | 1.709972 GiB | 1.705578 GiB |
| Rounded size | 1.7 GiB | 1.7 GiB |
| DuckDB block size | 262,144 bytes | 262,144 bytes |
| Total/used blocks | 7,004 / 7,004 | 6,986 / 6,986 |
| Free blocks | 0 | 0 |

The absolute difference is:

```text
1,836,068,864 - 1,831,350,272 = 4,718,592 bytes = 4.5 MiB = 18 DuckDB blocks
```

The relative reduction is approximately:

```text
4,718,592 / 1,836,068,864 × 100 = 0.257%
```

Thus, the result is **a very small reduction, not exactly equal sizes**. A display
rounded to one decimal GiB hides the difference.

### Timings from the saved logs

All values below are single-run observations, not medians or guaranteed performance.

| Measurement | 1.5.6 / v68 | 2.0.0 / v68 | 2.0.0 / v69 |
|---|---:|---:|---:|
| Connection creation | 0.0089 s | 0.0138 s | 0.0182 s |
| `SHOW TABLES` | 0.0019 s | 0.0063 s | 0.0063 s |
| `COUNT(*)` | 0.0016 s | 0.0008 s | 0.0009 s |
| Monthly aggregation | 0.6721 s | 0.1063 s | 0.2442 s |
| Shell `real` time | 1.081 s | 0.218 s | 0.367 s |
| Aggregation rows | 36 | 36 | 36 |
| Table rows | 50,000,000 | 50,000,000 | 50,000,000 |

For this one set of runs:

- DuckDB 2.0.0/v68 took **84.2% less aggregation time** than 1.5.6/v68, or about
  **6.32× less time**.
- DuckDB 2.0.0/v69 took **63.7% less aggregation time** than 1.5.6/v68, or about
  **2.75× less time**.
- DuckDB 2.0.0/v69 took **2.30× as long** as DuckDB 2.0.0/v68 for the aggregation.

All three logs show identical monthly aggregate values, which is the expected
correctness check for the conversion.

## Why the database size barely changed

### 1. A storage-version bump is not a universal compression guarantee

A new storage version allows DuckDB to change its physical representation and raise
the minimum reader version. It does not imply that every existing table will be
encoded differently or become smaller. The DuckDB 2.0 announcement highlights
features such as asynchronous I/O and native-format reads, but does not promise a
blanket file-size reduction for every workload.

The original size-reduction goal should therefore be treated as a **hypothesis to
measure**, not as guaranteed DuckDB 2.0 behavior.

### 2. Both files selected identical compression layouts

Inspecting both tables with:

```sql
SELECT
    column_name,
    compression,
    count(*) AS segments
FROM pragma_storage_info('flow_logs')
GROUP BY column_name, compression
ORDER BY column_name, compression;
```

shows identical compression methods and segment counts in v68 and v69:

- numeric and timestamp columns use the same `Constant` and `BitPacking` combinations;
- `year` and `month` use the same `Constant` and `RLE` combinations;
- `src_ip`, `dst_ip`, and `proto` use the same `Constant` and `DICT_FSST` combinations.

In particular, the v68 file already uses `DICT_FSST` for its string columns. There is
no additional string-encoding change in the converted file that would produce a
large saving for this dataset.

### 3. The source data was already compressed effectively

The v68 generator already benefits from columnar compression, timestamp ordering,
run-length encoding for grouped time fields, bit packing for numeric fields, and
dictionary/FSST encoding for strings. When the same logical rows are copied into a
format that selects the same encodings, a large further reduction is unlikely.

The random, high-cardinality IP addresses also offer less easy compression than
low-cardinality or highly repetitive text. Any storage improvement targeting other
data types or access patterns would not necessarily affect this table.

### 4. Allocation granularity hides small changes

DuckDB allocates this database in 256 KiB blocks. Small encoding differences can be
absorbed by partially used blocks and metadata. Conversely, the observed 18-block
saving is real but too small to materially change the footprint of a 1.71 GiB file.
Both databases have zero free blocks, so the similarity is not caused by retained
free space after deletes or updates.

## Why query performance looks similar—or inconsistent

### 1. The logs do not actually show one consistent performance result

DuckDB 2.0.0/v68 is substantially faster than the baseline in the saved aggregation
run, but v69 is slower than v68 under the same 2.0.0 engine. With only one run per
case, there is no variance estimate and no basis for attributing these differences
solely to the engine or file format.

A process restart also does not make the data “cold” at the operating-system level.
The first benchmark may populate the page cache, and later cases may reuse cached
pages. The fixed run order can therefore reward or penalize individual cases.

### 2. `COUNT(*)` is metadata-optimized

The 0.0008–0.0016 second row-count timings cannot represent reading 50 million rows
from disk. DuckDB can use catalog/table statistics for an unfiltered `COUNT(*)`.
Consequently, this measurement says almost nothing about storage scan throughput or
compression decode speed.

A real scan benchmark should perform work that requires reading column values, for
example `SELECT sum(byt) FROM flow_logs`, and should verify the operator plan where
necessary.

### 3. The aggregation exercises only three narrow columns

The monthly query reads `year`, `month`, and `byt`; it does not read the IP or protocol
strings. Improvements to string storage, `VARIANT`, nested data, or other DuckDB 2.0
features cannot help this query. Since the three queried columns use the same
compression schemes in both files, a large v68-versus-v69 difference is not expected.

With only 36 output groups, much of the work is scanning, decompressing, summing, and
hash-aggregating numeric values. This is not a comprehensive test of joins, filters,
string processing, zone-map pruning, large result materialization, or external I/O.

### 4. This local, cached workload does not isolate DuckDB 2.0's I/O improvements

DuckDB 2.0 introduces asynchronous I/O for native DuckDB storage as well as Parquet
and CSV. Its largest benefits are expected when storage latency or remote reads keep
the query processor waiting. A roughly 1.7 GiB local database that can be largely
cached in RAM does not reliably exercise that path.

If both formats have essentially the same bytes and encodings, warm cached queries
will primarily measure execution and decompression rather than storage-device read
performance.

### 5. Engine improvements are independent of file-format improvements

Any genuine gain between 1.5.6/v68 and 2.0.0/v68 comes without changing the file
format. Optimizer, execution, aggregation, build-toolchain, and other engine changes
can therefore affect the result independently of v69. Likewise, a v69-specific gain
must be established by comparing both files under the same DuckDB 2.0 engine with
repeated, controlled runs.

### 6. The harness includes more than database execution

The aggregation timer includes DuckDB-to-Polars conversion, while shell `real` time
also includes interpreter and environment startup. Those costs are small for this
36-row result but matter when comparing sub-second runs. They should be separated
from query execution before drawing precise performance conclusions.

## Assessment against the original objectives

| Objective | Evidence from this experiment | Assessment |
|---|---|---|
| Reduce database size with v69 | 4.5 MiB saved, or 0.257%; identical column compression layouts | **No meaningful reduction demonstrated** |
| Improve performance with DuckDB 2.0 | Faster single aggregation runs than the 1.5.6 baseline | **Promising observation, not yet statistically validated** |
| Improve performance specifically with v69 | Single v69 run was slower than 2.0.0/v68 and used the same encodings | **No v69-specific improvement demonstrated** |
| Preserve query results | Identical row counts, 36 aggregate rows, and monthly values | **Validated for the tested query** |
| Create a real v69 file | Storage tag is `v2.0.0+` | **Validated** |

The most accurate conclusion is therefore **not** “nothing changed.” The conversion
worked, the file became 0.257% smaller, and the recorded aggregation timings changed
substantially. Rather, the experiment does not demonstrate a material storage saving
or a reproducible v69 performance advantage for this dataset and query.

## Recommended next benchmark iteration

1. **Keep the three-case matrix.** It correctly separates engine and format effects.
2. **Run at least 10–20 measured iterations per case.** Report median, minimum,
   standard deviation or MAD, and preferably p95 rather than relying on one run.
3. **Interleave or randomize case order.** Avoid always running v68 before v69 and
   allowing cache order to determine the result.
4. **Separate cold and warm measurements.** A new Python process is not a cold OS
   cache. Use a dataset larger than RAM for I/O testing, or explicitly control OS
   caches only on a safe, dedicated benchmarking machine.
5. **Pin runtime settings.** Set the same `threads`, `memory_limit`, and temporary
   directory for every benchmark; record CPU, RAM, storage device, OS, and Python
   versions.
6. **Time DuckDB execution separately from Polars.** Materialize with `fetchall()` or
   another small built-in result for engine timing, then benchmark DataFrame conversion
   separately if it matters to the application.
7. **Replace the row-count “scan” with real work.** Add a full-column aggregation, a
   selective filter, a string-heavy query, and a join so that more storage paths are
   exercised.
8. **Inspect plans and profiles.** Use `EXPLAIN ANALYZE` to verify which columns and
   operators are actually executed and where time is spent.
9. **Test datasets with different characteristics.** Include repetitive strings,
   nullable columns, nested/`VARIANT` data, and less-ordered numeric data instead of
   relying only on this already well-compressed table.
10. **Retain raw results.** Save iteration-level timings as CSV or JSON, along with
    exact file sizes and storage metadata, so that later comparisons are reproducible.

## Compatibility notes

| Storage format | Introduced with | Minimum reader for these files | DuckDB 2.0 read support |
|---|---|---|---|
| v68 (`v1.5.0+`) | DuckDB 1.5 | DuckDB 1.5 or newer | Supported |
| v69 (`v2.0.0+`) | DuckDB 2.0 | DuckDB 2.0 or newer | Supported |

DuckDB versions before 2.0 cannot read the native v69 file. Keep `analytics.db` if
DuckDB 1.5 compatibility is required, and test migrations on copies rather than
production data.

## References

- [DuckDB 2.0.0 announcement and migration overview](https://duckdb.org/2026/10/21/announcing-duckdb-200.html)
- [DuckDB 2.0.0 release notes](https://github.com/duckdb/duckdb/releases/tag/v2.0.0)
- [DuckDB release calendar](https://duckdb.org/release_calendar)
