#!/usr/bin/env python3
import os
import time

import duckdb

DB_FILE = os.environ.get("DUCKDB_FILE", "/data/worker.duckdb")
HOST = os.environ.get("QUACK_HOST", "0.0.0.0")
PORT = int(os.environ.get("QUACK_PORT", "9494"))
TOKEN = os.environ.get("QUACK_TOKEN", "local-dev-token")

# quack_serve() rejects tokens shorter than 4 characters. When no usable token
# is configured we let the extension generate one and print it instead.
HAS_TOKEN = len(TOKEN) >= 4

connection = duckdb.connect(DB_FILE)

try:
    connection.execute("SET allow_unsigned_extensions=true")
except Exception:
    pass

connection.execute("INSTALL quack")
connection.execute("LOAD quack")

URI = f"quack:{HOST}:{PORT}"

print(
    f"Starting Quack server on {URI}, db={DB_FILE}",
    flush=True,
)

# The quack extension registers quack_serve() in the main schema; it takes a
# `quack:` URI rather than a database path. Binding a non-local hostname such
# as 0.0.0.0 is refused unless allow_other_hostname is set, which is required
# for the server to be reachable from other containers.
serve_sql = f"CALL quack_serve('{URI}', allow_other_hostname => true"

if HAS_TOKEN:
    safe_token = TOKEN.replace("'", "''")
    serve_sql += f", token => '{safe_token}'"

serve_sql += ")"

cursor = connection.execute(serve_sql)

columns = [d[0] for d in cursor.description] if cursor.description else []

for row in cursor.fetchall():
    for name, value in zip(columns, row):
        # Only redact a token we supplied ourselves; a generated one has to be
        # shown or clients cannot authenticate.
        if HAS_TOKEN and "token" in name.lower():
            value = "<redacted, set in QUACK_TOKEN>"

        print(f"  {name}: {value}", flush=True)

# quack_serve() spawns the listener on a background thread and returns, so the
# process must stay alive to keep serving.
while True:
    time.sleep(3600)
