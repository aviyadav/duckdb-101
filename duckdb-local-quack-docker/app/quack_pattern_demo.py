#!/usr/bin/env python3
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import duckdb


TOKEN = os.environ.get("QUACK_TOKEN", "local-dev-token")

ENDPOINTS = {
    "worker-1": os.environ.get("WORKER_1_ENDPOINT", "worker-1:9494"),
    "worker-2": os.environ.get("WORKER_2_ENDPOINT", "worker-2:9494"),
    "worker-3": os.environ.get("WORKER_3_ENDPOINT", "worker-3:9494"),
}

FRAGMENTS = [
    (
        "worker-1",
        """
        SELECT sale_status, COUNT(*) AS count_star
        FROM sales
        GROUP BY sale_status
        ORDER BY sale_status
        """,
    ),
    (
        "worker-2",
        """
        SELECT country, COUNT(*) AS count_star
        FROM customers
        GROUP BY country
        ORDER BY country
        """,
    ),
    (
        "worker-3",
        """
        SELECT category, COUNT(*) AS count_star
        FROM products
        GROUP BY category
        ORDER BY category
        """,
    ),
]


def sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def quack_uri(endpoint: str) -> str:
    if endpoint.startswith("quack:"):
        return endpoint

    return f"quack:{endpoint}"


def redact_token(text: str) -> str:
    if TOKEN:
        return text.replace(TOKEN, "REDACTED")
    return text


def run_fragment(seq, worker_id, sql, barrier, epoch):
    endpoint = ENDPOINTS[worker_id]

    barrier.wait()

    started = time.perf_counter()

    error = None
    columns = ()
    rows = []

    attach_sql = ""
    remote_sql = ""

    try:
        with duckdb.connect() as connection:
            try:
                connection.execute("SET allow_unsigned_extensions=true")
            except Exception:
                pass

            connection.execute("INSTALL quack")
            connection.execute("LOAD quack")

            safe_token = TOKEN.replace("'", "''")

            attach_sql = (
                f"ATTACH '{quack_uri(endpoint)}' AS remote "
                f"(TYPE quack, TOKEN '{safe_token}', DISABLE_SSL true)"
            )

            remote_sql = f"SELECT * FROM remote.query({sql_literal(sql)})"

            print(
                f"[query-{seq}] {worker_id}: {redact_token(attach_sql)}",
                flush=True,
            )

            connection.execute(attach_sql)

            cursor = connection.execute(remote_sql)

            if cursor.description:
                columns = tuple(d[0] for d in cursor.description)
                rows = cursor.fetchall()
            else:
                columns = ("Count",)
                rows = [(1,)]

    except Exception as exc:
        error = str(exc)

    finished = time.perf_counter()

    return {
        "seq": seq,
        "worker_id": worker_id,
        "label": f"query-{seq}",
        "sql": sql.strip(),
        "attach_sql": redact_token(attach_sql),
        "remote_sql": remote_sql,
        "start_offset_ms": (started - epoch) * 1000.0,
        "duration_ms": (finished - started) * 1000.0,
        "columns": columns,
        "rows": rows,
        "error": error,
    }


def print_table(columns, rows, max_rows=20):
    if not columns:
        print("No result")
        return

    columns = [str(c) for c in columns]

    visible_rows = [
        [str(v) if v is not None else "NULL" for v in row]
        for row in rows[:max_rows]
    ]

    widths = [
        max(
            len(columns[i]),
            max((len(row[i]) for row in visible_rows), default=0),
        )
        for i in range(len(columns))
    ]

    print("  ".join(c.ljust(widths[i]) for i, c in enumerate(columns)))
    print("  ".join("-" * w for w in widths))

    for row in visible_rows:
        print("  ".join(row[i].ljust(widths[i]) for i in range(len(columns))))

    if len(rows) > max_rows:
        print(f"... {len(rows) - max_rows} more rows")


def main():
    barrier = threading.Barrier(len(FRAGMENTS))
    epoch = time.perf_counter()

    with ThreadPoolExecutor(max_workers=len(FRAGMENTS)) as pool:
        futures = []

        for seq, (worker_id, sql) in enumerate(FRAGMENTS, start=1):
            futures.append(
                pool.submit(
                    run_fragment,
                    seq,
                    worker_id,
                    sql,
                    barrier,
                    epoch,
                )
            )

        results = [future.result() for future in futures]

    print("\nConcurrent remote queries using Quack article pattern")

    print(
        f"{'worker':<10} "
        f"{'table':<10} "
        f"{'start_offset_ms':>16} "
        f"{'duration_seconds':>18}"
    )

    print(f"{'-' * 10} {'-' * 10} {'-' * 16} {'-' * 18}")

    ordered_by_start = sorted(results, key=lambda r: r["start_offset_ms"])

    for r in ordered_by_start:
        print(
            f"{r['worker_id']:<10} "
            f"{r['label']:<10} "
            f"{r['start_offset_ms']:>16.3f} "
            f"{r['duration_ms'] / 1000.0:>18.3f}"
        )

    if results:
        spread = max(r["start_offset_ms"] for r in results) - min(
            r["start_offset_ms"] for r in results
        )
        print(f"\nStart spread: {spread:.3f} ms")

    for r in sorted(results, key=lambda x: x["seq"]):
        print(f"\n{r['label']} ({r['worker_id']})")

        print("Coordinator-side Quack pattern:")
        print(r["attach_sql"])
        print(r["remote_sql"])

        if r["error"]:
            print(f"ERROR: {r['error']}")
        else:
            print("Result:")
            print_table(r["columns"], r["rows"])


if __name__ == "__main__":
    main()