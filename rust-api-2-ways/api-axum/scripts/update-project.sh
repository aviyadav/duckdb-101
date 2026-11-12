#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:3000}"
TOKEN="${TOKEN:?Set TOKEN env var}"
PROJECT_ID="${1:?Usage: $0 <project_id> [name] [description]}"
NAME="${2:-Renamed Project}"
DESCRIPTION="${3:-Updated description}"

curl -sS -X PATCH "$BASE_URL/v1/projects/$PROJECT_ID" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d "{\"name\":\"$NAME\",\"description\":\"$DESCRIPTION\"}"
echo
