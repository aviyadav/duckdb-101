#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:3001}"
TOKEN="${TOKEN:?Set TOKEN env var}"
PROJECT_ID="${1:?Usage: $0 <project_id>}"

curl -sS "$BASE_URL/v1/projects/$PROJECT_ID" \
  -H "Authorization: Bearer $TOKEN"
echo
