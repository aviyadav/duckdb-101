#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:3000}"
TOKEN="${TOKEN:?Set TOKEN env var (e.g. TOKEN=\$(./scripts/login.sh | sed -n 's/.*\"token\":\"\([^\"]*\)\".*/\1/p'))}"

curl -sS "$BASE_URL/v1/projects" \
  -H "Authorization: Bearer $TOKEN"
echo
