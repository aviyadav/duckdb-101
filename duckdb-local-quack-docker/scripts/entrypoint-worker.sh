#!/bin/sh
set -e

: "${WORKER_INDEX:?WORKER_INDEX is required}"
: "${DUCKDB_FILE:=/data/worker.duckdb}"
: "${ROW_COUNT:=100000}"
: "${SERVER_MODE:=http}"

mkdir -p "$(dirname "$DUCKDB_FILE")"

if [ "${FORCE_SEED:-0}" = "1" ] || [ ! -f "${DUCKDB_FILE}.seeded" ]; then
    echo "Seeding worker ${WORKER_INDEX} with ${ROW_COUNT} rows into ${DUCKDB_FILE}"
    python /app/seed_related_data.py \
        --worker "$WORKER_INDEX" \
        --rows "$ROW_COUNT" \
        --database "$DUCKDB_FILE"

    touch "${DUCKDB_FILE}.seeded"
fi

if [ "$SERVER_MODE" = "quack" ]; then
    echo "Starting Quack server mode"
    exec python /app/quack_server.py
else
    echo "Starting local HTTP SQL server mode"
    exec python /app/http_sql_server.py
fi