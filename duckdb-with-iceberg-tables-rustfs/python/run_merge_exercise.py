#!/usr/bin/env python3
"""
Run the "DuckDB MERGE on Iceberg tables" exercise against the *local* stack.

Original article (AWS S3 Tables + Glue):
    https://medium.com/@shahsoumil519/duckdb-now-supports-merge-on-iceberg-tables-4e4363b925c4

Local equivalent provided by this repo (see docker-compose.yml):
    RustFS      -> S3-compatible object storage  (replaces AWS S3 / S3 Tables storage)
    Lakekeeper  -> Iceberg REST catalog          (replaces Glue / S3 Tables REST API)
    PostgreSQL  -> catalog metadata database     (replaces the Glue Data Catalog)

The script executes every file in ../sql in alphabetical order, substituting
placeholders from the environment (defaults match docker-compose.yml and
config/create-warehouse.json) and printing each result set.

Usage:
    python python/run_merge_exercise.py
"""

from __future__ import annotations

import os
import pathlib
import string

import duckdb

ROOT = pathlib.Path(__file__).resolve().parents[1]
SQL_DIR = ROOT / "sql"

# Defaults mirror docker-compose.yml / config/create-warehouse.json.
# Export any of these variables to override them.
DEFAULTS = {
    "RUSTFS_ACCESS_KEY": "lakehouse-admin",
    "RUSTFS_SECRET_KEY": "lakehouse-admin-secret",
    "S3_REGION": "us-east-1",
    "S3_ENDPOINT": "host.docker.internal:9000",  # RustFS S3 API, host:port, no scheme
    "CATALOG_ENDPOINT": "http://localhost:8181/catalog",  # Lakekeeper Iceberg REST (note the /catalog base path)
    "ICEBERG_TOKEN": "dummy",             # unsecured local catalog
}


def sql_files() -> list[pathlib.Path]:
    """All exercise SQL files, in execution order."""
    return sorted(SQL_DIR.glob("*.sql"))


def render(sql: str) -> str:
    """Substitute ${VARS} using the environment, falling back to DEFAULTS."""
    values = {**DEFAULTS, **{k: v for k, v in os.environ.items() if k in DEFAULTS}}
    return string.Template(sql).substitute(values)


def print_rows(columns: list[str], rows: list[tuple]) -> None:
    """Small aligned table printer (avoids a pandas dependency)."""
    if not columns:
        return
    cells = [[("" if v is None else str(v)) for v in row] for row in rows]
    widths = [
        max(len(columns[i]), *(len(r[i]) for r in cells)) if cells
        else len(columns[i])
        for i in range(len(columns))
    ]
    print("  " + " | ".join(c.ljust(widths[i]) for i, c in enumerate(columns)))
    print("  " + "-+-".join("-" * w for w in widths))
    for row in cells:
        print("  " + " | ".join(v.ljust(widths[i]) for i, v in enumerate(row)))
    print(f"  ({len(rows)} row{'s' if len(rows) != 1 else ''})")


def run_file(con: duckdb.DuckDBPyConnection, path: pathlib.Path) -> None:
    """Execute one SQL file and print the result of its final statement."""
    print(f"\n=== {path.name} " + "=" * max(3, 60 - len(path.name)))
    con.execute(render(path.read_text(encoding="utf-8")))
    if con.description:  # the file ended in a SELECT
        columns = [d[0] for d in con.description]
        print_rows(columns, con.fetchall())


def main() -> int:
    con = duckdb.connect()

    print("Extensions in use")
    con.execute("INSTALL httpfs; LOAD httpfs; INSTALL iceberg; LOAD iceberg;")
    print_rows(
        ["extension_name", "extension_version"],
        con.sql(
            "SELECT extension_name, extension_version FROM duckdb_extensions() "
            "WHERE extension_name IN ('iceberg', 'httpfs') ORDER BY extension_name"
        ).fetchall(),
    )
    print(f"duckdb (pip) version: {duckdb.__version__}")
    print(f"catalog endpoint: {os.environ.get('CATALOG_ENDPOINT', DEFAULTS['CATALOG_ENDPOINT'])}")
    print(f"S3 endpoint:      {os.environ.get('S3_ENDPOINT', DEFAULTS['S3_ENDPOINT'])}")

    for path in sql_files():
        run_file(con, path)

    print("\nDone: all MERGE statements committed as Iceberg snapshots.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
