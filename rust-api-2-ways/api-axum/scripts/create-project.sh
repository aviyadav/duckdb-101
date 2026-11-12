#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:3000}"
TOKEN="${TOKEN:?Set TOKEN env var}"
NAME="${1:-My Project}"
DESCRIPTION="${2:-Testing SQLite}"

curl -sS -X POST "$BASE_URL/v1/projects" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d "{\"name\":\"$NAME\",\"description\":\"$DESCRIPTION\"}"
echo
