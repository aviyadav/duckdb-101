#!/usr/bin/env python3
"""Query the Dockerised DuckDB workers over the plain HTTP JSON protocol.

Standard library only: HTTP mode needs no pip installs on the host at all.
The workers must be running with SERVER_MODE=http.

As a script:

    python local/http_client.py --health
    python local/http_client.py --query "worker-1=SELECT COUNT(*) FROM sales"
    python local/http_client.py --allow-write --query "worker-1=DELETE FROM sales WHERE sale_id = 1"

As a library:

    import sys; sys.path.insert(0, "local")
    from http_client import HttpClient

    client = HttpClient("worker-1")
    print(client.query("SELECT sale_status, COUNT(*) FROM sales GROUP BY 1"))
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.request
from pathlib import Path

DEFAULT_TOKEN = "local-dev-token"
DEFAULT_HOST = "localhost"
DEFAULT_TIMEOUT = 60.0

# Host ports published by docker-compose.yml; every worker listens on 9494
# inside its own container.
DEFAULT_PORTS = {
    "worker-1": 9491,
    "worker-2": 9492,
    "worker-3": 9493,
}


class WorkerError(RuntimeError):
    """A worker was unreachable, or returned a non-2xx status / error payload."""


def endpoint_for(worker: str) -> str:
    """Resolve a worker label to a host-reachable ``host:port`` endpoint.

    Honours the same ``WORKER_N_ENDPOINT`` variables the in-container
    coordinator reads, so one set of exports works for both.
    """
    env_key = worker.strip().upper().replace("-", "_") + "_ENDPOINT"

    override = os.environ.get(env_key)
    if override:
        return override

    if worker not in DEFAULT_PORTS:
        raise WorkerError(
            f"Unknown worker {worker!r}. Expected one of "
            f"{', '.join(sorted(DEFAULT_PORTS))}, or set {env_key}."
        )

    return f"{DEFAULT_HOST}:{DEFAULT_PORTS[worker]}"


def normalize_http_url(endpoint: str) -> str:
    if endpoint.startswith(("http://", "https://")):
        return endpoint

    return f"http://{endpoint}"


def strip_sql(sql: str) -> str:
    sql = sql.strip()

    while sql.endswith(";"):
        sql = sql[:-1].rstrip()

    return sql


class HttpClient:
    """Minimal client for one worker's HTTP JSON SQL endpoint."""

    def __init__(self, worker, token=None, timeout=DEFAULT_TIMEOUT):
        self.worker = worker
        self.endpoint = endpoint_for(worker)
        self.base_url = normalize_http_url(self.endpoint)
        self.timeout = timeout

        if token is None:
            token = os.environ.get("QUACK_TOKEN", DEFAULT_TOKEN)

        self.token = token

    def _headers(self):
        headers = {"Content-Type": "application/json"}

        if self.token:
            headers["X-Quack-Token"] = self.token

        return headers

    def health(self):
        """GET /health - works without a token."""
        request = urllib.request.Request(
            f"{self.base_url}/health",
            headers=self._headers(),
            method="GET",
        )

        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                return json.loads(response.read().decode())
        except urllib.error.HTTPError as exc:
            raise WorkerError(f"HTTP {exc.code} from {self.base_url}/health") from None
        except urllib.error.URLError as exc:
            raise WorkerError(
                f"cannot reach {self.worker} at {self.base_url}: {exc.reason}. "
                "Is the container up, and is SERVER_MODE=http?"
            ) from None

    def query(self, sql, allow_write=False):
        """POST /query and return ``{"columns": [...], "rows": [[...]]}``.

        Without ``allow_write`` the worker opens the database read-only, so
        INSERT / UPDATE / DELETE / DDL will fail.
        """
        sql = strip_sql(sql)

        if not sql:
            raise WorkerError("empty SQL")

        payload = json.dumps({"sql": sql, "allow_write": allow_write}).encode()

        request = urllib.request.Request(
            f"{self.base_url}/query",
            data=payload,
            headers=self._headers(),
            method="POST",
        )

        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                data = json.loads(response.read().decode())
        except urllib.error.HTTPError as exc:
            body = exc.read().decode()

            if exc.code == 401:
                raise WorkerError(
                    f"{self.worker} rejected the token (HTTP 401). "
                    "Check QUACK_TOKEN matches .env."
                ) from None

            try:
                raise WorkerError(json.loads(body).get("error") or body) from None
            except json.JSONDecodeError:
                raise WorkerError(body or f"HTTP {exc.code}") from None
        except urllib.error.URLError as exc:
            raise WorkerError(
                f"cannot reach {self.worker} at {self.base_url}: {exc.reason}. "
                "Is the container up, and is SERVER_MODE=http?"
            ) from None

        if isinstance(data, dict) and data.get("error") and "columns" not in data:
            raise WorkerError(data["error"])

        return data


def print_result(result, max_rows=100):
    columns = [str(c) for c in result.get("columns", [])]
    rows = result.get("rows", [])

    if not columns:
        print("No result")
        return

    visible = [
        ["NULL" if value is None else str(value) for value in row]
        for row in rows[:max_rows]
    ]

    widths = [
        max(len(columns[i]), max((len(row[i]) for row in visible), default=0))
        for i in range(len(columns))
    ]

    print("  ".join(c.ljust(widths[i]) for i, c in enumerate(columns)))
    print("  ".join("-" * w for w in widths))

    for row in visible:
        print("  ".join(row[i].ljust(widths[i]) for i in range(len(columns))))

    if len(rows) > max_rows:
        print(f"... {len(rows) - max_rows} more rows")


def split_assignment(value):
    if "=" not in value:
        raise WorkerError(f"expected worker-N=SQL, got {value!r}")

    worker, rest = value.split("=", 1)

    return worker.strip().lower(), rest.strip()


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Query the Docker DuckDB workers over HTTP (SERVER_MODE=http).",
    )

    parser.add_argument(
        "--health",
        action="store_true",
        help="Probe /health on every worker and exit.",
    )
    parser.add_argument(
        "--query",
        action="append",
        default=[],
        metavar="WORKER=SQL",
        help='Repeatable, e.g. --query "worker-1=SELECT COUNT(*) FROM sales".',
    )
    parser.add_argument(
        "--query-file",
        action="append",
        default=[],
        metavar="WORKER=PATH",
        help="Repeatable. Read the SQL from a file on the host.",
    )
    parser.add_argument(
        "--allow-write",
        action="store_true",
        help="Permit INSERT, UPDATE, DELETE and DDL.",
    )
    parser.add_argument("--token", default=None, help="Defaults to $QUACK_TOKEN.")
    parser.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT)
    parser.add_argument("--max-rows", type=int, default=100)

    args = parser.parse_args(argv)

    if args.health:
        failures = 0

        for worker in sorted(DEFAULT_PORTS):
            client = HttpClient(worker, token=args.token, timeout=args.timeout)

            try:
                payload = client.health()
                print(f"{worker:<10} {client.base_url}/health  OK    {payload}")
            except WorkerError as exc:
                failures += 1
                print(f"{worker:<10} {client.base_url}/health  FAIL  {exc}")

        return 1 if failures else 0

    queries = []

    try:
        for item in args.query:
            worker, sql = split_assignment(item)
            queries.append((worker, sql))

        for item in args.query_file:
            worker, path = split_assignment(item)
            queries.append((worker, Path(path).expanduser().read_text(encoding="utf-8")))
    except (WorkerError, OSError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if not queries:
        parser.print_help()
        return 1

    failures = 0

    for worker, sql in queries:
        client = HttpClient(worker, token=args.token, timeout=args.timeout)

        print(f"\n=== {worker}  {client.base_url}/query ===")
        print(strip_sql(sql))
        print()

        try:
            print_result(client.query(sql, allow_write=args.allow_write), args.max_rows)
        except WorkerError as exc:
            failures += 1
            print(f"ERROR: {exc}")

    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
