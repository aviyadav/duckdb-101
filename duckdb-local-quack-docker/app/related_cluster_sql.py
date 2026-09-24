#!/usr/bin/env python3
import argparse
import json
import os
import sys
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path


@dataclass
class QueryFragment:
    worker_id: str
    label: str
    sql: str
    seq: int


def endpoint_for(worker_id: str) -> str:
    env_key = worker_id.strip().upper().replace("-", "_") + "_ENDPOINT"
    default_port = os.environ.get("QUACK_PORT", "9494")

    return os.environ.get(
        env_key,
        f"{worker_id}:{default_port}",
    )


def normalize_http_url(endpoint: str) -> str:
    if endpoint.startswith(("http://", "https://")):
        return endpoint

    return f"http://{endpoint}"


def normalize_quack_uri(endpoint: str) -> str:
    if endpoint.startswith("quack:"):
        return endpoint

    return f"quack:{endpoint}"


def sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def strip_sql(sql: str) -> str:
    sql = sql.strip()

    while sql.endswith(";"):
        sql = sql[:-1].rstrip()

    return sql


def split_assignment(value: str):
    if "=" not in value:
        raise ValueError(f"Expected worker-N=SQL, got: {value!r}")

    worker, rest = value.split("=", 1)
    worker = worker.strip().lower()

    if not worker.startswith("worker-"):
        raise ValueError(
            "Worker label must look like worker-1, worker-2, worker-3; "
            f"got {worker!r}"
        )

    return worker, rest.strip()


def http_executor(endpoint: str, token: str, allow_write: bool):
    url = normalize_http_url(endpoint) + "/query"

    headers = {
        "Content-Type": "application/json",
        "X-Quack-Token": token,
    }

    def executor(sql: str):
        payload = json.dumps(
            {
                "sql": sql,
                "allow_write": allow_write,
            }
        ).encode()

        request = urllib.request.Request(
            url,
            data=payload,
            headers=headers,
            method="POST",
        )

        try:
            with urllib.request.urlopen(request, timeout=3600) as response:
                data = json.loads(response.read().decode())

                if isinstance(data, dict) and data.get("error") and "columns" not in data:
                    raise RuntimeError(data["error"])

                return data

        except urllib.error.HTTPError as e:
            body = e.read().decode()

            try:
                parsed = json.loads(body)
                raise RuntimeError(parsed.get("error", body))
            except json.JSONDecodeError:
                raise RuntimeError(body)

    return executor


def quack_executor(endpoint: str, token: str):
    def executor(sql: str):
        import duckdb

        safe_token = token.replace("'", "''")

        with duckdb.connect() as connection:
            try:
                connection.execute("SET allow_unsigned_extensions=true")
            except Exception:
                pass

            connection.execute("INSTALL quack")
            connection.execute("LOAD quack")

            connection.execute(
                f"ATTACH '{normalize_quack_uri(endpoint)}' AS remote "
                f"(TYPE quack, TOKEN '{safe_token}', DISABLE_SSL true)"
            )

            cursor = connection.execute(
                f"SELECT * FROM remote.query({sql_literal(sql)})"
            )

            if cursor.description:
                columns = tuple(d[0] for d in cursor.description)
                rows = cursor.fetchall()
            else:
                columns = ("Count",)
                rows = [(1,)]

            return {
                "columns": columns,
                "rows": rows,
            }

    return executor


def wait_http(endpoint: str, timeout: int) -> bool:
    url = normalize_http_url(endpoint) + "/health"
    deadline = time.time() + timeout

    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=2) as response:
                if response.status == 200:
                    return True
        except Exception:
            pass

        time.sleep(1)

    return False


def wait_quack(endpoint: str, token: str, timeout: int) -> bool:
    deadline = time.time() + timeout
    executor = quack_executor(endpoint, token)

    while time.time() < deadline:
        try:
            executor("SELECT 1 AS ok")
            return True
        except Exception:
            time.sleep(1)

    return False


def wait_for_workers(fragments, mode, token, timeout):
    workers = sorted({fragment.worker_id for fragment in fragments})

    for worker_id in workers:
        endpoint = endpoint_for(worker_id)

        print(f"Waiting for {worker_id} at {endpoint} ...", flush=True)

        if mode == "http":
            ok = wait_http(endpoint, timeout)
        else:
            ok = wait_quack(endpoint, token, timeout)

        if not ok:
            print(
                f"Timed out waiting for {worker_id} at {endpoint}",
                file=sys.stderr,
            )
            sys.exit(1)


def make_executor(fragment, mode, token, allow_write):
    endpoint = endpoint_for(fragment.worker_id)

    if mode == "http":
        return http_executor(endpoint, token, allow_write)

    return quack_executor(endpoint, token)


def run_fragment(fragment, executor, barrier, epoch):
    barrier.wait()

    started = time.perf_counter()

    try:
        raw_result = executor(fragment.sql)
        error = None
    except Exception as exc:
        raw_result = None
        error = str(exc)

    finished = time.perf_counter()

    return {
        "fragment": fragment,
        "start_offset_ms": (started - epoch) * 1000.0,
        "duration_ms": (finished - started) * 1000.0,
        "result": raw_result,
        "error": error,
    }


def fmt(value):
    if value is None:
        return "NULL"

    text = str(value)

    if len(text) > 60:
        return text[:57] + "..."

    return text


def print_table(result, max_rows):
    if not result:
        print("No result")
        return

    columns = result.get("columns", [])
    rows = result.get("rows", [])

    if not columns:
        print("No result")
        return

    columns = [str(c) for c in columns]

    visible_rows = [
        [fmt(v) for v in row]
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
    parser = argparse.ArgumentParser(
        description="Run SQL fragments concurrently across DuckDB workers."
    )

    parser.add_argument(
        "--query",
        action="append",
        default=[],
        help="worker-1=SELECT ...",
    )

    parser.add_argument(
        "--query-file",
        action="append",
        default=[],
        help="worker-1=/path/file.sql",
    )

    parser.add_argument(
        "--allow-write",
        action="store_true",
        help="Permit INSERT, UPDATE, DELETE, DDL.",
    )

    parser.add_argument(
        "--show-sql",
        action="store_true",
        help="Show SQL with each result.",
    )

    parser.add_argument(
        "--mode",
        choices=["http", "quack"],
        default=os.environ.get("SERVER_MODE", "http"),
    )

    parser.add_argument(
        "--wait-timeout",
        type=int,
        default=180,
    )

    parser.add_argument(
        "--max-rows",
        type=int,
        default=100,
    )

    args = parser.parse_args()

    fragments = []

    def add_fragment(worker_id, sql):
        sql = strip_sql(sql)

        if not sql:
            raise ValueError("Empty SQL fragment")

        seq = len(fragments) + 1

        fragments.append(
            QueryFragment(
                worker_id=worker_id,
                label=f"query-{seq}",
                sql=sql,
                seq=seq,
            )
        )

    try:
        for item in args.query:
            worker_id, sql = split_assignment(item)
            add_fragment(worker_id, sql)

        for item in args.query_file:
            worker_id, path = split_assignment(item)
            sql = Path(path).expanduser().read_text()
            add_fragment(worker_id, sql)

    except Exception as exc:
        print(f"error: {exc}", file=sys.stderr)
        sys.exit(1)

    if not fragments:
        parser.print_help()
        sys.exit(1)

    token = os.environ.get("QUACK_TOKEN", "local-dev-token")

    wait_for_workers(
        fragments,
        args.mode,
        token,
        args.wait_timeout,
    )

    barrier = threading.Barrier(len(fragments))
    epoch = time.perf_counter()

    with ThreadPoolExecutor(max_workers=len(fragments)) as pool:
        futures = {}

        for fragment in fragments:
            executor = make_executor(
                fragment,
                args.mode,
                token,
                args.allow_write,
            )

            futures[
                pool.submit(
                    run_fragment,
                    fragment,
                    executor,
                    barrier,
                    epoch,
                )
            ] = fragment

        results = []

        for future in futures:
            results.append(future.result())

    print("\nConcurrent remote queries")
    print(
        f"{'worker':<10} "
        f"{'table':<10} "
        f"{'start_offset_ms':>16} "
        f"{'duration_seconds':>18}"
    )
    print(f"{'-'*10} {'-'*10} {'-'*16} {'-'*18}")

    ordered_by_start = sorted(
        results,
        key=lambda r: r["start_offset_ms"],
    )

    for r in ordered_by_start:
        fragment = r["fragment"]

        print(
            f"{fragment.worker_id:<10} "
            f"{fragment.label:<10} "
            f"{r['start_offset_ms']:>16.3f} "
            f"{r['duration_ms'] / 1000.0:>18.3f}"
        )

    if ordered_by_start:
        spread = (
            ordered_by_start[-1]["start_offset_ms"]
            - ordered_by_start[0]["start_offset_ms"]
        )

        print(f"\nStart spread: {spread:.3f} ms")

    for r in sorted(results, key=lambda x: x["fragment"].seq):
        fragment = r["fragment"]

        print(f"\n{fragment.label} ({fragment.worker_id})")

        if args.show_sql:
            print("SQL:")
            print(fragment.sql)
            print("Result:")

        if r["error"]:
            print(f"ERROR: {r['error']}")
        else:
            print_table(r["result"], args.max_rows)


if __name__ == "__main__":
    main()