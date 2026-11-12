#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:3000}"
PASS=0
FAIL=0

assert_status() {
  local name="$1"
  local expected="$2"
  local actual="$3"
  local body="${4:-}"

  if [ "$actual" = "$expected" ]; then
    echo "[PASS] $name (HTTP $actual)"
    PASS=$((PASS + 1))
  else
    echo "[FAIL] $name: expected HTTP $expected, got HTTP $actual"
    if [ -n "$body" ]; then
      echo "       Response: $body"
    fi
    FAIL=$((FAIL + 1))
  fi
}

request() {
  local method="$1"
  local path="$2"
  local token="${3:-}"
  local data="${4:-}"

  local args=(-sS -w "\n%{http_code}" -X "$method" "$BASE_URL$path")
  if [ -n "$token" ]; then
    args+=(-H "Authorization: Bearer $token")
  fi
  if [ -n "$data" ]; then
    args+=(-H "Content-Type: application/json" -d "$data")
  fi

  curl "${args[@]}"
}

echo "Testing API at $BASE_URL"
echo "----------------------------------------"

# 1. OpenAPI spec
resp=$(request GET "/api-docs/openapi.json")
status=$(echo "$resp" | tail -n1)
body=$(echo "$resp" | sed '$d')
assert_status "GET /api-docs/openapi.json" "200" "$status" "$body"

# 2. Unauthorized access check
resp=$(request GET "/v1/projects")
status=$(echo "$resp" | tail -n1)
body=$(echo "$resp" | sed '$d')
assert_status "GET /v1/projects without token" "401" "$status" "$body"

# 3. Login with invalid credentials
resp=$(request POST "/v1/auth/login" "" '{"email":"test@example.com","password":"wrong"}')
status=$(echo "$resp" | tail -n1)
body=$(echo "$resp" | sed '$d')
assert_status "POST /v1/auth/login (invalid password)" "401" "$status" "$body"

# 4. Login with valid credentials
resp=$(request POST "/v1/auth/login" "" '{"email":"test@example.com","password":"password"}')
status=$(echo "$resp" | tail -n1)
body=$(echo "$resp" | sed '$d')
assert_status "POST /v1/auth/login (valid)" "200" "$status" "$body"

TOKEN=$(echo "$body" | sed -n 's/.*"token":"\([^"]*\)".*/\1/p')
if [ -z "$TOKEN" ]; then
  echo "[FAIL] Could not extract JWT token from login response"
  exit 1
fi

# 5. Create project - validation error (name < 3 chars)
resp=$(request POST "/v1/projects" "$TOKEN" '{"name":"ab"}')
status=$(echo "$resp" | tail -n1)
body=$(echo "$resp" | sed '$d')
assert_status "POST /v1/projects (validation error)" "400" "$status" "$body"

# 6. Create project - valid
resp=$(request POST "/v1/projects" "$TOKEN" '{"name":"Script Test Project","description":"Created by test-all.sh"}')
status=$(echo "$resp" | tail -n1)
body=$(echo "$resp" | sed '$d')
assert_status "POST /v1/projects (create)" "201" "$status" "$body"

PROJECT_ID=$(echo "$body" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')
if [ -z "$PROJECT_ID" ]; then
  echo "[FAIL] Could not extract project ID from create response"
  exit 1
fi

# 7. List projects
resp=$(request GET "/v1/projects" "$TOKEN")
status=$(echo "$resp" | tail -n1)
body=$(echo "$resp" | sed '$d')
assert_status "GET /v1/projects (list)" "200" "$status" "$body"

# 8. Get project by ID
resp=$(request GET "/v1/projects/$PROJECT_ID" "$TOKEN")
status=$(echo "$resp" | tail -n1)
body=$(echo "$resp" | sed '$d')
assert_status "GET /v1/projects/$PROJECT_ID" "200" "$status" "$body"

# 9. Update project
resp=$(request PATCH "/v1/projects/$PROJECT_ID" "$TOKEN" '{"name":"Updated Test Project","description":"Updated by test-all.sh"}')
status=$(echo "$resp" | tail -n1)
body=$(echo "$resp" | sed '$d')
assert_status "PATCH /v1/projects/$PROJECT_ID" "200" "$status" "$body"

# 10. Delete project
resp=$(request DELETE "/v1/projects/$PROJECT_ID" "$TOKEN")
status=$(echo "$resp" | tail -n1)
body=$(echo "$resp" | sed '$d')
assert_status "DELETE /v1/projects/$PROJECT_ID" "204" "$status" "$body"

# 11. Get deleted project (expect 404)
resp=$(request GET "/v1/projects/$PROJECT_ID" "$TOKEN")
status=$(echo "$resp" | tail -n1)
body=$(echo "$resp" | sed '$d')
assert_status "GET /v1/projects/$PROJECT_ID (after delete)" "404" "$status" "$body"

echo "----------------------------------------"
echo "Results: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
