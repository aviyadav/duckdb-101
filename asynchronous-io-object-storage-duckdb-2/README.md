# VPC Flow Logs with DuckDB, Parquet, and RUSTFS

This project contains two related workflows:

| Script | Purpose |
| --- | --- |
| `get_oct_2026.py` | Discovers October 2026 Parquet files in RUSTFS through its S3-compatible API and aggregates them with DuckDB. The original AWS implementation is retained as comments. |
| `get_oct_26_v2.py` | DuckDB 2.0 version of the RUSTFS query, using the native `CREATE SECRET` credential API. |
| `generate_and_upload_parquet.py` | Generates synthetic October 2026 flow logs as local Parquet files and uploads them to a local RUSTFS server. |

The generator preserves the AWS account ID, regions, bucket names, and object prefixes used by the original script.

## Requirements

- Python 3.15 or newer
- DuckDB 2.0.0 (installed by `uv sync`)
- [uv](https://docs.astral.sh/uv/)
- RUSTFS running at `http://localhost:9001` for uploads and remote queries
- RUSTFS console available at `http://localhost:9001/rustfs/console`

Install the Python dependencies:

```sh
uv sync
```

## Configure RUSTFS credentials

Create a local environment file:

```sh
cp .env.example .env
```

Update `.env` with the RUSTFS console admin credentials:

```dotenv
RUSTFS_ADMIN=your_admin
RUSTFS_PASSWORD=your_password
RUSTFS_ENDPOINT_URL=http://localhost:9001
```

`RUSTFS_ACCESS_KEY` and `RUSTFS_SECRET_KEY` are also accepted as alternatives to `RUSTFS_ADMIN` and `RUSTFS_PASSWORD`.

The endpoint is the S3 API address. Do not append `/rustfs/console` to `RUSTFS_ENDPOINT_URL`. The `.env` file is excluded by `.gitignore`; do not commit credentials.

## Generate and upload Parquet files

Run:

```sh
uv run generate_and_upload_parquet.py
```

The program will:

1. Generate **250,000 synthetic flow-log records** using a deterministic random seed.
2. Write **125,000 records for each configured region**.
3. Create **10 Snappy-compressed Parquet files**, each containing 25,000 records.
4. Save the files beneath the local `data/` directory.
5. Read the RUSTFS credentials from `.env`.
6. Create either destination bucket if it does not already exist.
7. Upload every generated file to the corresponding bucket and object key.

Running the command again regenerates the local files and overwrites objects with the same keys in RUSTFS.

### Region and bucket allocation

| Region | Records | Bucket |
| --- | ---: | --- |
| `eu-central-1` | 125,000 | `ec1-vpc-flowlogs-bucket` |
| `eu-west-1` | 125,000 | `ew1-vpc-flowlogs-bucket` |

### Local and remote object layout

Local files follow this pattern:

```text
data/<bucket>/prod/AWSLogs/015443280042/vpcflowlogs/<region>/2026/10/<region>_<file-number>.parquet
```

For example:

```text
data/ec1-vpc-flowlogs-bucket/prod/AWSLogs/015443280042/vpcflowlogs/eu-central-1/2026/10/eu-central-1_00000.parquet
```

The same object is uploaded as:

```text
s3://ec1-vpc-flowlogs-bucket/prod/AWSLogs/015443280042/vpcflowlogs/eu-central-1/2026/10/eu-central-1_00000.parquet
```

Each region produces files numbered `00000` through `00004`. The generated `data/` directory is excluded by `.gitignore`.

## Parquet schema

The files use the standard 14-field VPC Flow Log structure with typed Parquet columns:

| Column | Type | Description |
| --- | --- | --- |
| `version` | `INT32` | VPC Flow Log version; always `2`. |
| `account_id` | `STRING` | AWS account ID; always `015443280042`. |
| `interface_id` | `STRING` | Synthetic elastic network interface ID. |
| `srcaddr` | `STRING` | Source IP address. |
| `dstaddr` | `STRING` | Destination IP address. |
| `srcport` | `INT32` | Source port. |
| `dstport` | `INT32` | Destination port. |
| `protocol` | `INT32` | IANA protocol number, such as TCP `6` or UDP `17`. |
| `packets` | `INT64` | Number of packets in the flow. |
| `bytes` | `INT64` | Number of bytes in the flow. |
| `start` | `INT64` | Flow start time as a Unix timestamp in seconds. |
| `end` | `INT64` | Flow end time as a Unix timestamp in seconds. |
| `action` | `STRING` | `ACCEPT` or `REJECT`. |
| `log_status` | `STRING` | Flow log status; always `OK`. |

Flow start timestamps fall within October 2026. All records are synthetic and do not contain real AWS network data.

## Query RUSTFS Parquet files with DuckDB

After generating and uploading the dataset, run the DuckDB 2.0 version:

```sh
uv run get_oct_26_v2.py
```

The script uses the same `.env` credentials as the generator. It discovers `.parquet` objects with boto3, creates a DuckDB 2.0 native S3 secret for path-style HTTP access to RUSTFS, and prints the top 20 source/destination pairs by total bytes. It also checks the active DuckDB version and stops if it is not 2.0.

The earlier `get_oct_2026.py` script provides the same RUSTFS aggregation using DuckDB's legacy `SET s3_*` configuration statements:

```sh
uv run get_oct_2026.py
```

No AWS credentials or AWS profile are needed for either active code path.

## Query local Parquet files with DuckDB

The following example reproduces the source/destination traffic aggregation without requiring AWS or RUSTFS access:

```sh
uv run python
```

```python
import duckdb

con = duckdb.connect()
rows = con.execute(
    """
    SELECT
        srcaddr AS source_ip,
        dstaddr AS destination_ip,
        SUM(bytes) AS total_bytes,
        COUNT(*) AS total_records
    FROM read_parquet('data/**/*.parquet')
    GROUP BY srcaddr, dstaddr
    ORDER BY total_bytes DESC
    LIMIT 20
    """
).fetchall()

for row in rows:
    print(row)
```

To query one region, narrow the glob to its bucket and prefix, for example:

```python
rows = con.execute(
    """
    SELECT COUNT(*)
    FROM read_parquet(
        'data/ec1-vpc-flowlogs-bucket/prod/AWSLogs/015443280042/vpcflowlogs/eu-central-1/2026/10/*.parquet'
    )
    """
).fetchone()
print(rows[0])  # 125000
```

## Original AWS S3 code

The original AWS implementations in `get_oct_2026.py` and `get_oct_26_v2.py` are commented out rather than deleted. They include:

- AWS profile authentication with `flowlog_bot`
- AWS session creation and credential forwarding to DuckDB
- Discovery of gzip-compressed, space-delimited `.log.gz` files
- The original `read_csv` query and VPC Flow Log column mapping

The active code uses the same account ID, buckets, regions, and October 2026 prefixes, but reads the generated Parquet data from RUSTFS instead.

## Benchmark DuckDB 1.5.6 and 2.0

Use `uv run --with` to temporarily select either DuckDB version without changing `pyproject.toml` or recreating the project environment.

Warm up the package and DuckDB extension caches before measuring:

```sh
uv run --with 'duckdb==1.5.6' get_oct_2026.py
uv run --with 'duckdb==2.0.0' get_oct_26_v2.py
```

Capture wall-clock timings and output logs:

```sh
{ time uv run --with 'duckdb==1.5.6' get_oct_2026.py; } 2>&1 | tee duckdb-1.5.6.log
{ time uv run --with 'duckdb==2.0.0' get_oct_26_v2.py; } 2>&1 | tee duckdb-2.0.0.log
```

Each run reports two useful measurements:

- `Total Runtime`: the script's internal measurement, covering object discovery and querying.
- `real`: total wall-clock time, including `uv`, Python startup, and imports.

Compare the internal timings from the saved logs:

```sh
grep 'Total Runtime' duckdb-1.5.6.log duckdb-2.0.0.log
```

For repeated runs and an automatic relative-speed report, use [Hyperfine](https://github.com/sharkdp/hyperfine):

```sh
hyperfine --warmup 1 --runs 5 \
  "uv run --with 'duckdb==1.5.6' get_oct_2026.py" \
  "uv run --with 'duckdb==2.0.0' get_oct_26_v2.py"
```

For a fair comparison, configure the RUSTFS `.env` credentials, use the same remote objects for both versions, and do not regenerate the dataset between runs.

## Checks

Lint all scripts and verify the dependency lock file with:

```sh
uv run ruff check generate_and_upload_parquet.py get_oct_2026.py get_oct_26_v2.py
uv lock --check
```
