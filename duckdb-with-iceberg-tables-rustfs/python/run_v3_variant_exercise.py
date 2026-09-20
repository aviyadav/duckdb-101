#!/usr/bin/env python3
"""
Run exercise 2: "DuckDB reads Iceberg v3 VARIANT" against the *local* stack.

Article (AWS S3 Tables):
    https://medium.com/@shahsoumil519/can-duckdb-read-iceberg-v3-variant-from-amazon-s3-tables-quick-test-a191ab70bf80

Local equivalent (same docker-compose.yml stack as exercise 1):
    RustFS      -> S3-compatible object storage   (replaces the S3 Tables storage)
    Lakekeeper  -> Iceberg REST catalog           (replaces the S3 Tables REST API)
    PostgreSQL  -> catalog metadata database      (replaces the Glue Data Catalog)

The script executes every file in ../sql-v3-variant in alphabetical order,
substitutes placeholders from the environment (defaults match docker-compose.yml
and config/create-warehouse.json) and prints the last result set of each file.

Unlike the article - which needed `FORCE INSTALL iceberg FROM core_nightly` - the
stable DuckDB >= 1.5.3 `iceberg` extension already reads *and writes* v3 tables.

Usage:
    python python/run_v3_variant_exercise.py
"""

from __future__ import annotations

import os
import pathlib
import string
import sys

import duckdb

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))  # reuse the helpers of exercise 1

from run_merge_exercise import DEFAULTS, print_rows  # noqa: E402

ROOT = HERE.parent
SQL_DIR = ROOT / "sql-v3-variant"

# Placeholders that only exist in this exercise.
DEFAULTS_V3 = {
    # NDJSON sample with GH-Archive-shaped events (swap for a downloaded hour).
    "GH_JSON_PATH": (ROOT / "data" / "sample_github_events.ndjson").as_posix(),
}


def render(sql: str) -> str:
    """Substitute placeholders using the environment, falling back to defaults."""
    known = {**DEFAULTS, **DEFAULTS_V3}
    values = {**known, **{k: v for k, v in os.environ.items() if k in known}}
    return string.Template(sql).substitute(values)


def run_file(con: duckdb.DuckDBPyConnection, path: pathlib.Path) -> None:
    """Execute one SQL file and print the result of its final statement."""
    print(f"\n=== {path.name} " + "=" * max(3, 60 - len(path.name)))
    con.execute(render(path.read_text(encoding="utf-8")))
    if con.description:  # the file ended in a SELECT / DESCRIBE
        print_rows([d[0] for d in con.description], con.fetchall())


def main() -> int:
    con = duckdb.connect()

    print("Exercise 2: Iceberg format v3 + VARIANT (RustFS storage, PostgreSQL catalog)")
    con.execute(
        "INSTALL httpfs;  LOAD httpfs;"
        "INSTALL iceberg; LOAD iceberg;"
        "INSTALL json;    LOAD json;"
    )
    print_rows(
        ["extension_name", "extension_version"],
        con.sql(
            "SELECT extension_name, extension_version FROM duckdb_extensions() "
            "WHERE extension_name IN ('iceberg', 'httpfs', 'json') ORDER BY extension_name"
        ).fetchall(),
    )
    print(f"duckdb (pip) version: {duckdb.__version__}")
    print(f"catalog endpoint: {os.environ.get('CATALOG_ENDPOINT', DEFAULTS['CATALOG_ENDPOINT'])}")
    print(f"S3 endpoint:      {os.environ.get('S3_ENDPOINT', DEFAULTS['S3_ENDPOINT'])}")
    print(f"sample events:    {os.environ.get('GH_JSON_PATH', DEFAULTS_V3['GH_JSON_PATH'])}")

    for path in sorted(SQL_DIR.glob("*.sql")):
        run_file(con, path)

    print("\nDone: Iceberg v3 VARIANT read back from RustFS via the PostgreSQL-backed catalog.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
