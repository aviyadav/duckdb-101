#!/usr/bin/env python3
"""
generate_flow_logs.py
=====================
Data generator for the DuckDB flow-log benchmark (`benchmark.py`).

It builds `analytics.db` containing ONE table, `flow_logs`, with the columns
the benchmark needs (`year`, `month`, `byt`) plus realistic NetFlow-style
context columns:

    flow_id     BIGINT      sequential id
    ts          TIMESTAMP   flow start time (random instant inside its month)
    year        SMALLINT    year(ts)   -- kept in sync with ts
    month       UTINYINT    month(ts)  -- kept in sync with ts
    src_ip      VARCHAR     internal 10.x.x.x source
    dst_ip      VARCHAR     ~30% internal, ~70% public-looking
    src_port    INTEGER     ephemeral port 1024..65535
    dst_port    INTEGER     weighted common service ports (443/80/53/22/...)
    proto       VARCHAR     TCP ~82%, UDP ~15%, ICMP ~3%
    packets     BIGINT      ~ byt / random avg packet size (64..1500)
    byt         BIGINT      heavy-tailed flow size (see below)
    duration_ms INTEGER     log-uniform 5 ms .. 10 min

Data characteristics (so the benchmark is meaningful, not trivial):
  * Time window: `--months` calendar months (default 36) ending at
    `--end-month` (default: last complete month). Recent months get more
    rows (mild growth bias), so GROUP BY year, month returns a nice curve.
  * Flow sizes are a log-uniform mixture: 90% small (200 B .. 2 MB),
    9% medium (2 .. 100 MB), 1% elephant flows (100 MB .. 5 GB), inflated
    ~4%/month over the window. Total volume lands in the multi-TB range
    at the default 50M rows, so the GB aggregation is non-trivial.
  * Rows are inserted in ts order per batch -> good zone maps + compression.

Storage format version (important!)
-----------------------------------
Official DuckDB storage-version table:
    v64 -> DuckDB 0.9 .. 1.1 (also the DEFAULT for 1.2 .. 1.5)
    v65 -> 1.2.x   v66 -> 1.3.x   v67 -> 1.4.x   v68 -> 1.5.x   v69 -> 2.0.x
A file's storage version is the MINIMUM DuckDB release that can read it,
so a true v68 file requires duckdb >= 1.5 and CANNOT be opened by 1.4.x.

  --storage-version auto      (default) newest format the installed duckdb
                              can write: 1.4.x -> v67, 1.5.x -> v68, ...
  --storage-version v1.4.0    explicit v67 (readable by DuckDB >= 1.4.0)
  --storage-version v1.5.0    explicit v68 (needs `pip install -U duckdb`)

Usage
-----
  python3 generate_flow_logs.py                            # 50M rows, auto
  python3 generate_flow_logs.py --rows 5000000 --force     # quick demo
  python3 generate_flow_logs.py --rows 200000000 --batch 2000000 \
      --memory-limit 4GB --threads 8 --force               # big machine
  python3 generate_flow_logs.py --storage-version v1.5.0 --force  # true v68

Requires: duckdb >= 1.2 (STORAGE_VERSION attach option). No pandas needed
here; the benchmark itself needs pandas for .df().
"""

import argparse
import datetime as dt
import os
import re
import sys
import time

import duckdb

# release string -> public storage version number (duckdb.org internals/storage)
PUBLIC_STORAGE_NUMBER = {
    "v0.10.0": 64, "v1.0.0": 64, "v1.1.0": 64,
    "v1.2.0": 65, "v1.3.0": 66, "v1.4.0": 67, "v1.5.0": 68, "v2.0.0": 69,
}

TABLE_DDL = """
CREATE TABLE flow_logs (
    flow_id     BIGINT,
    ts          TIMESTAMP,
    year        SMALLINT,
    month       UTINYINT,
    src_ip      VARCHAR,
    dst_ip      VARCHAR,
    src_port    INTEGER,
    dst_port    INTEGER,
    proto       VARCHAR,
    packets     BIGINT,
    byt         BIGINT,
    duration_ms INTEGER
)
"""

# One batch = INSERT ... SELECT over range(start, stop). Everything is drawn
# server-side in DuckDB (no Python row loops), so generation is fast.
BATCH_SQL = """
INSERT INTO flow_logs (flow_id, ts, year, month, src_ip, dst_ip,
                       src_port, dst_port, proto, packets, byt, duration_ms)
WITH base AS (
    SELECT
        i AS flow_id,
        -- bias toward recent months: x^0.75 pushes uniform draws upward
        floor({months} * pow(random(), 0.75))::INT AS m
    FROM range({start}, {stop}) t(i)
),
stamped AS (
    SELECT
        flow_id, m,
        (DATE '{first_month}-01' + to_months(m))::TIMESTAMP
            + to_days(floor(random() * day(last_day(
                  DATE '{first_month}-01' + to_months(m))))::INT)
            + to_seconds(floor(random() * 86400)::INT) AS ts
    FROM base
),
sized AS (
    SELECT
        flow_id, m, ts,
        random() AS proto_r,
        -- heavy-tailed log-uniform mixture, growing ~4% per month index
        CASE
            WHEN random() < 0.90 THEN exp(ln(200.0) + random() * (ln(2.0e6) - ln(200.0)))
            WHEN random() < 0.99 THEN exp(ln(2.0e6) + random() * (ln(1.0e8) - ln(2.0e6)))
            ELSE                      exp(ln(1.0e8) + random() * (ln(5.0e9) - ln(1.0e8)))
        END * (1.0 + 0.04 * m) AS raw_byt
    FROM stamped
)
SELECT
    flow_id,
    ts,
    year(ts)::SMALLINT,
    month(ts)::UTINYINT,
    '10.' || floor(random() * 256)::INT || '.'
          || floor(random() * 256)::INT || '.'
          || (1 + floor(random() * 254)::INT),
    CASE WHEN random() < 0.30
         THEN '10.0.' || floor(random() * 256)::INT || '.'
                      || (1 + floor(random() * 254)::INT)
         ELSE (1 + floor(random() * 223)::INT) || '.'
              || floor(random() * 256)::INT || '.'
              || floor(random() * 256)::INT || '.'
              || (1 + floor(random() * 254)::INT)
    END,
    (1024 + floor(random() * 64512))::INT,
    ([443, 443, 443, 443, 80, 80, 53, 22,
      25, 110, 143, 3389, 8080, 3306, 5432, 6379])[1 + floor(random() * 16)::INT],
    CASE WHEN proto_r < 0.82 THEN 'TCP'
         WHEN proto_r < 0.97 THEN 'UDP'
         ELSE 'ICMP' END,
    greatest(1, (raw_byt / (64.0 + random() * 1436.0))::BIGINT),
    greatest(64, raw_byt::BIGINT),
    greatest(1, exp(ln(5.0) + random() * (ln(600000.0) - ln(5.0)))::INTEGER)
FROM sized
ORDER BY ts
"""


def parse_args():
    p = argparse.ArgumentParser(
        description="Generate analytics.db (flow_logs) for the DuckDB benchmark.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--db", default="analytics.db", help="output database path")
    p.add_argument("--rows", type=int, default=50_000_000, help="number of flow rows")
    p.add_argument("--months", type=int, default=36, help="months of history")
    p.add_argument("--end-month", default=None, metavar="YYYY-MM",
                   help="last month of the window (default: previous calendar month)")
    p.add_argument("--batch", type=int, default=1_000_000, help="rows per INSERT batch")
    p.add_argument("--seed", type=float, default=0.42,
                   help="RNG seed (approx. reproducible; exact with --threads 1)")
    p.add_argument("--storage-version", default="auto",
                   help="'auto' = newest format this duckdb can write "
                        "(1.4.x->v67, 1.5.x->v68); or explicit e.g. v1.4.0 / v1.5.0")
    p.add_argument("--memory-limit", default="1GB", help="DuckDB memory_limit")
    p.add_argument("--threads", type=int, default=None, help="DuckDB threads (default: all)")
    p.add_argument("--force", action="store_true", help="overwrite an existing database")
    return p.parse_args()


def month_window(months: int, end_month: str | None):
    """Return (first_month 'YYYY-MM', end_month 'YYYY-MM')."""
    if end_month is None:
        last_prev = dt.date.today().replace(day=1) - dt.timedelta(days=1)
        end = last_prev.replace(day=1)
    else:
        if not re.fullmatch(r"\d{4}-\d{2}", end_month):
            sys.exit(f"--end-month must look like YYYY-MM, got {end_month!r}")
        end = dt.datetime.strptime(end_month, "%Y-%m").date()
    y, m = end.year, end.month
    total = y * 12 + (m - 1) - (months - 1)
    first = dt.date(total // 12, total % 12 + 1, 1)
    return first.strftime("%Y-%m"), end.strftime("%Y-%m")


def auto_storage_version() -> str:
    m = re.match(r"(\d+)\.(\d+)", duckdb.__version__)
    return f"v{m.group(1)}.{m.group(2)}.0" if m else "latest"


def human(n: float) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(n) < 1024 or unit == "TB":
            return f"{n:,.2f} {unit}"
        n /= 1024


def main():
    args = parse_args()
    if args.rows < 1 or args.batch < 1 or args.months < 1:
        sys.exit("--rows, --batch and --months must be >= 1")

    db_path = os.path.abspath(args.db)
    if os.path.exists(db_path):
        if not args.force:
            sys.exit(f"[!] {db_path} already exists. Use --force to overwrite.")
        for suffix in ("", ".wal"):
            try:
                os.remove(db_path + suffix)
            except FileNotFoundError:
                pass

    first_month, end_month = month_window(args.months, args.end_month)
    storage_version = auto_storage_version() if args.storage_version == "auto" \
        else args.storage_version

    print("================ [flow_logs data generator] ================")
    print(f"DuckDB library      : {duckdb.__version__}")
    print(f"Output database     : {db_path}")
    print(f"Rows                : {args.rows:,}  (batch {args.batch:,})")
    print(f"Time window         : {first_month} .. {end_month}  ({args.months} months)")
    print(f"Requested storage   : {storage_version}"
          + (f"  (public v{PUBLIC_STORAGE_NUMBER[storage_version]})"
             if storage_version in PUBLIC_STORAGE_NUMBER else ""))
    print(f"Memory limit        : {args.memory_limit}")

    con = duckdb.connect()  # in-memory driver; target file is ATTACHed
    con.execute(f"SET memory_limit = '{args.memory_limit}'")
    if args.threads:
        con.execute(f"SET threads = {args.threads}")
    con.execute("SET preserve_insertion_order = false")
    con.execute(f"SELECT setseed({args.seed})")

    escaped = db_path.replace("'", "''")
    try:
        con.execute(f"ATTACH '{escaped}' AS target (STORAGE_VERSION '{storage_version}')")
    except duckdb.InvalidInputException as e:
        con.close()
        sys.exit(f"[!] Cannot write storage version {storage_version!r} with "
                 f"duckdb {duckdb.__version__}.\n    {e}\n"
                 f"    Hint: v68 ('v1.5.0') needs duckdb>=1.5  ->  pip install -U duckdb\n"
                 f"    DuckDB 1.4.x can write/read at most v67 ('v1.4.0').")
    except duckdb.IOException as e:
        con.close()
        sys.exit(f"[!] Attach failed: {e}")
    con.execute("USE target")
    con.execute(TABLE_DDL)

    t0 = time.perf_counter()
    done = 0
    while done < args.rows:
        n = min(args.batch, args.rows - done)
        sql = BATCH_SQL.format(months=args.months, start=done, stop=done + n,
                               first_month=first_month)
        bt = time.perf_counter()
        con.execute(sql)
        done += n
        rate = n / (time.perf_counter() - bt)
        print(f"  inserted {done:>13,} / {args.rows:,} rows   "
              f"({rate:,.0f} rows/s last batch)", flush=True)

    print("-> CHECKPOINT (flush WAL into the main database file)...")
    con.execute("CHECKPOINT target")
    gen_time = time.perf_counter() - t0

    # ---- summary -----------------------------------------------------------
    rows, ts_min, ts_max, n_months, total_byt = con.execute("""
        SELECT count(*), min(ts), max(ts),
               count(DISTINCT year::INTEGER * 100 + month), sum(byt)
        FROM flow_logs
    """).fetchone()
    tag = con.execute("""
        SELECT tags['storage_version'] FROM duckdb_databases()
        WHERE database_name = 'target'
    """).fetchone()[0]
    con.close()  # clean close -> no leftover WAL, safe for read_only benchmark

    release = (tag or "").rstrip("+")
    pub = PUBLIC_STORAGE_NUMBER.get(release)
    size = os.path.getsize(db_path)
    print("\n---------------- [generation complete] ----------------")
    print(f"  Time                : {gen_time:.1f} s  ({rows / gen_time:,.0f} rows/s)")
    print(f"  Table               : flow_logs  ({rows:,} rows, {n_months} months)")
    print(f"  ts range            : {ts_min}  ..  {ts_max}")
    print(f"  Total volume        : {human(total_byt)}  (sum of byt)")
    print(f"  File size           : {human(size)}")
    print(f"  Storage version     : {tag}" + (f"  ->  public v{pub}" if pub else ""))
    if pub == 67:
        print("  Note: v67 is the DuckDB 1.4.x format. True v68 needs duckdb>=1.5")
        print("        (--storage-version v1.5.0); 1.4.x cannot open v68 files.")
    print("--------------------------------------------------------")
    print(f"[+] Ready. Run the benchmark with:  python3 benchmark.py")


if __name__ == "__main__":
    main()
