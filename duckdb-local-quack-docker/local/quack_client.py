#!/usr/bin/env python3
"""Query the Dockerised DuckDB workers through the real Quack protocol.

Requires duckdb on the host, and the version must match the one baked into the
image (see local/requirements.txt) or the quack extension will not load.
The workers must be running with SERVER_MODE=quack.

As a script:

    python local/quack_client.py --check
    python local/quack_client.py --query "worker-1=SELECT COUNT(*) FROM sales"
    python local/quack_client.py --attach-all --sql "SELECT * FROM w1.sales LIMIT 5"
    python local/quack_client.py --attach-all --tables

As a library:

    import sys; sys.path.insert(0, "local")
    from quack_client import QuackClient

    with QuackClient() as client:
        client.attach_all()
        print(client.fetch("SELECT COUNT(*) FROM w1.sales"))
"""

from __future__ import annotations

import argparse
import os
import sys

try:
    import duckdb
except ImportError:
    sys.exit(
        "duckdb is not installed on this host. Run:\n"
        "    python -m venv .venv && . .venv/bin/activate\n"
        "    pip install -r local/requirements.txt"
    )

DEFAULT_TOKEN = "local-dev-token"
DEFAULT_HOST = "localhost"

# Host ports published by docker-compose.yml; every worker listens on 9494
# inside its own container.
DEFAULT_PORTS = {
    "worker-1": 9491,
    "worker-2": 9492,
    "worker-3": 9493,
}

# "worker-1" is not a valid SQL identifier, so each worker gets a short alias
# once attached.
ALIASES = {
    "worker-1": "w1",
    "worker-2": "w2",
    "worker-3": "w3",
}


class QuackError(RuntimeError):
    """A worker could not be attached, or a remote query failed."""


def endpoint_for(worker):
    """Resolve a worker label to a host-reachable ``host:port`` endpoint.

    Honours the same ``WORKER_N_ENDPOINT`` variables the in-container
    coordinator reads, so one set of exports works for both.
    """
    env_key = worker.strip().upper().replace("-", "_") + "_ENDPOINT"

    override = os.environ.get(env_key)
    if override:
        return override

    if worker not in DEFAULT_PORTS:
        raise QuackError(
            f"Unknown worker {worker!r}. Expected one of "
            f"{', '.join(sorted(DEFAULT_PORTS))}, or set {env_key}."
        )

    return f"{DEFAULT_HOST}:{DEFAULT_PORTS[worker]}"


def quack_uri(endpoint):
    """Quack endpoints need the ``quack:`` scheme or they read as file paths."""
    if endpoint.startswith("quack:"):
        return endpoint

    return f"quack:{endpoint}"


def alias_for(worker):
    if worker in ALIASES:
        return ALIASES[worker]

    raise QuackError(f"No catalog alias defined for {worker!r}")


def sql_literal(value):
    return "'" + value.replace("'", "''") + "'"


def resolve_token(token=None):
    if token is not None:
        return token

    return os.environ.get("QUACK_TOKEN", DEFAULT_TOKEN)


def connect():
    """Open a local in-memory DuckDB session with the quack extension loaded."""
    connection = duckdb.connect()

    try:
        connection.execute("SET allow_unsigned_extensions=true")
    except Exception:
        pass

    try:
        connection.execute("INSTALL quack")
        connection.execute("LOAD quack")
    except Exception as exc:
        raise QuackError(
            f"could not install/load the quack extension for duckdb "
            f"{duckdb.__version__}: {exc}. INSTALL quack needs outbound "
            "internet on first run, and the host duckdb version must match "
            "the one in the image (see local/requirements.txt)."
        ) from None

    return connection


class QuackClient:
    """A local DuckDB session with remote workers attached as catalogs."""

    def __init__(self, token=None, connection=None):
        self.token = resolve_token(token)
        self.connection = connection if connection is not None else connect()
        self.attached = {}

    def attach(self, worker, alias=None):
        """ATTACH one worker and return the catalog alias it was given."""
        alias = alias or alias_for(worker)
        uri = quack_uri(endpoint_for(worker))
        safe_token = self.token.replace("'", "''")

        try:
            self.connection.execute(
                f"ATTACH '{uri}' AS {alias} "
                f"(TYPE quack, TOKEN '{safe_token}', DISABLE_SSL true)"
            )
        except Exception as exc:
            raise QuackError(
                f"could not attach {worker} at {uri}: {exc}. "
                "Is the container up, and is SERVER_MODE=quack?"
            ) from None

        self.attached[worker] = alias

        return alias

    def attach_all(self):
        for worker in sorted(DEFAULT_PORTS):
            self.attach(worker)

        return dict(self.attached)

    def execute(self, sql):
        return self.connection.execute(sql)

    def fetch(self, sql):
        """Run SQL locally and return ``(columns, rows)``."""
        cursor = self.connection.execute(sql)

        if not cursor.description:
            return (), []

        return tuple(d[0] for d in cursor.description), cursor.fetchall()

    def query_worker(self, worker, sql):
        """Push arbitrary SQL down to one worker via its ``query`` macro."""
        alias = self.attached.get(worker) or self.attach(worker)

        return self.fetch(f"SELECT * FROM {alias}.query({sql_literal(sql)})")

    def close(self):
        self.connection.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self.close()


def print_table(columns, rows, max_rows=100):
    if not columns:
        print("No result")
        return

    columns = [str(c) for c in columns]

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
        raise QuackError(f"expected worker-N=SQL, got {value!r}")

    worker, rest = value.split("=", 1)

    return worker.strip().lower(), rest.strip()


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Query the Docker DuckDB workers over Quack (SERVER_MODE=quack).",
    )

    parser.add_argument(
        "--check",
        action="store_true",
        help="Load the quack extension, attach every worker, then exit.",
    )
    parser.add_argument(
        "--query",
        action="append",
        default=[],
        metavar="WORKER=SQL",
        help='Repeatable. Pushed down via <alias>.query(...).',
    )
    parser.add_argument(
        "--sql",
        action="append",
        default=[],
        metavar="SQL",
        help="Repeatable. Run against the local session; reference w1/w2/w3.",
    )
    parser.add_argument(
        "--tables",
        action="store_true",
        help="List the tables visible in every attached catalog.",
    )
    parser.add_argument(
        "--attach-all",
        action="store_true",
        help="Attach worker-1/2/3 up front as w1/w2/w3.",
    )
    parser.add_argument("--token", default=None, help="Defaults to $QUACK_TOKEN.")
    parser.add_argument("--max-rows", type=int, default=100)

    args = parser.parse_args(argv)

    try:
        with QuackClient(token=args.token) as client:
            print(
                f"host duckdb {duckdb.__version__}, "
                f"quack loaded, token {'set' if client.token else 'empty'}"
            )

            attached = {}

            if args.attach_all or args.check or args.tables or args.sql:
                attached = client.attach_all()

                for worker, alias in attached.items():
                    print(f"attached {worker:<10} as {alias}  ({quack_uri(endpoint_for(worker))})")

            if args.check:
                for worker in sorted(attached):
                    columns, rows = client.query_worker(worker, "SELECT 1 AS ok")
                    print(f"{worker:<10} SELECT 1 -> {rows}")

                return 0

            failures = 0

            if args.tables:
                columns, rows = client.fetch(
                    "SELECT database_name, schema_name, table_name "
                    "FROM duckdb_tables() "
                    "WHERE database_name IN ('w1', 'w2', 'w3') "
                    "ORDER BY 1, 2, 3"
                )
                print()
                print_table(columns, rows, args.max_rows)

            for item in args.query:
                worker, sql = split_assignment(item)

                print(f"\n=== {worker} ===")
                print(sql.strip())
                print()

                try:
                    columns, rows = client.query_worker(worker, sql)
                    print_table(columns, rows, args.max_rows)
                except Exception as exc:
                    failures += 1
                    print(f"ERROR: {exc}")

            for sql in args.sql:
                print("\n=== local session ===")
                print(sql.strip())
                print()

                try:
                    columns, rows = client.fetch(sql)
                    print_table(columns, rows, args.max_rows)
                except Exception as exc:
                    failures += 1
                    print(f"ERROR: {exc}")

            if not (args.tables or args.query or args.sql):
                parser.print_help()
                return 1

            return 1 if failures else 0

    except QuackError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
