#!/bin/bash
set -euo pipefail

SQL_FILE="/sql/${QUACK_ROLE}.sql"

if [[ ! -f "$SQL_FILE" ]]; then
  echo "Missing SQL file for role: ${QUACK_ROLE}" >&2
  exit 1
fi

if [[ "${QUACK_ROLE}" == "coordinator" ]]; then
  for target in server1 server2; do
    token_var="${target^^}_TOKEN"
    token="${!token_var}"
    uri="quack://${target}:9494"
    echo "Waiting for ${target}..."
    for _ in $(seq 1 60); do
      if duckdb -c "FROM quack_query('${uri}', 'SELECT 1', token='${token}', disable_ssl => true);" >/dev/null 2>&1; then
        echo "${target} is ready"
        break
      fi
      sleep 1
    done
  done
fi

echo "Starting ${QUACK_ROLE}..."
{
  cat "$SQL_FILE"
  tail -f /dev/null
} | duckdb -box
