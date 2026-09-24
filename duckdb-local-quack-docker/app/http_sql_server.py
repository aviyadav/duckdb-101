#!/usr/bin/env python3
import json
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

import duckdb

DB_FILE = os.environ.get("DUCKDB_FILE", "/data/worker.duckdb")
PORT = int(os.environ.get("QUACK_PORT", "9494"))
TOKEN = os.environ.get("QUACK_TOKEN", "").strip()

WRITE_LOCK = threading.RLock()


class Server(ThreadingHTTPServer):
    daemon_threads = True


def _fetch(cursor):
    if cursor.description:
        return {
            "columns": [d[0] for d in cursor.description],
            "rows": cursor.fetchall(),
        }

    try:
        rows = cursor.fetchall()
        if rows:
            return {
                "columns": ["Count"],
                "rows": rows,
            }
    except Exception:
        pass

    return {
        "columns": ["Count"],
        "rows": [[1]],
    }


def run_sql(sql: str, allow_write: bool):
    sql = sql.strip()

    while sql.endswith(";"):
        sql = sql[:-1].rstrip()

    if not sql:
        raise ValueError("empty SQL")

    if allow_write:
        with WRITE_LOCK:
            connection = duckdb.connect(DB_FILE)
            try:
                cursor = connection.execute(sql)
                return _fetch(cursor)
            finally:
                connection.close()
    else:
        connection = duckdb.connect(DB_FILE, read_only=True)
        try:
            cursor = connection.execute(sql)
            return _fetch(cursor)
        finally:
            connection.close()


class Handler(BaseHTTPRequestHandler):
    def _send(self, code, payload):
        body = json.dumps(payload, default=str).encode()

        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _authorized(self):
        if not TOKEN:
            return True

        if self.headers.get("X-Quack-Token", "") == TOKEN:
            return True

        auth = self.headers.get("Authorization", "")
        return auth == f"Bearer {TOKEN}"

    def do_GET(self):
        path = urlparse(self.path).path

        if path == "/health":
            self._send(
                200,
                {
                    "status": "ok",
                    "db": DB_FILE,
                },
            )
        else:
            self._send(404, {"error": "not found"})

    def do_POST(self):
        path = urlparse(self.path).path

        if path != "/query":
            self._send(404, {"error": "not found"})
            return

        if not self._authorized():
            self._send(401, {"error": "invalid or missing token"})
            return

        try:
            length = int(self.headers.get("Content-Length", 0))
            payload = json.loads(self.rfile.read(length).decode() or "{}")

            sql = payload.get("sql", "")
            allow_write = bool(payload.get("allow_write", False))

            result = run_sql(sql, allow_write)

            self._send(200, result)
        except Exception as exc:
            self._send(400, {"error": str(exc)})

    def log_message(self, fmt, *args):
        print("[http-sql] " + fmt % args, flush=True)


if __name__ == "__main__":
    print(
        f"HTTP SQL server listening on 0.0.0.0:{PORT}, db={DB_FILE}",
        flush=True,
    )

    Server(
        ("0.0.0.0", PORT),
        Handler,
    ).serve_forever()