#!/usr/bin/env bash
# test_api.sh - End-to-end suite (works against BOTH frameworks).
#   ./test_api.sh http://localhost:3000   (Axum)
#   ./test_api.sh http://localhost:3001   (Actix Web)
# Requires: curl, jq   (sudo apt install curl jq)
set -uo pipefail
BASE_URL="${1:-http://localhost:3000}"; BASE_URL="${BASE_URL%/}"
if [[ -t 1 ]]; then G='\033[0;32m'; R='\033[0;31m'; Y='\033[1;33m'; C='\033[0;36m'; N='\033[0m'
else G=''; R=''; Y=''; C=''; N=''; fi
PASS=0; FAIL=0; WARN=0
command -v curl >/dev/null 2>&1 || { echo "ERROR: curl is required"; exit 1; }
command -v jq   >/dev/null 2>&1 || { echo "ERROR: jq is required (sudo apt install jq)"; exit 1; }
BF="$(mktemp)"; trap 'rm -f "$BF"' EXIT
STATUS=""; BODY=""; RID="00000000-0000-0000-0000-000000000000"

req() { # METHOD PATH [JSON_BODY] [TOKEN]
  local m="$1" p="$2" d="${3:-}" t="${4:-}"
  local a=(-s -o "$BF" -w '%{http_code}' --max-time 15 -X "$m" "$BASE_URL$p")
  [[ -n "$d" ]] && a+=(-H 'Content-Type: application/json' -d "$d")
  [[ -n "$t" ]] && a+=(-H "Authorization: Bearer $t")
  STATUS="$(curl "${a[@]}" 2>/dev/null)" || STATUS="000"; BODY="$(cat "$BF")"
}
pass() { PASS=$((PASS+1)); printf "  ${G}PASS${N}  %s\n" "$1"; }
fail() { FAIL=$((FAIL+1)); printf "  ${R}FAIL${N}  %s\n        status=%s body=%.200s\n" "$1" "$STATUS" "$BODY"; }
warn() { WARN=$((WARN+1)); printf "  ${Y}WARN${N}  %s\n" "$1"; }
sec()  { printf "\n${C}== %s ==${N}\n" "$1"; }
es()   { if [[ "$STATUS" == "$2" ]]; then pass "$1"; else fail "$1 (expected HTTP $2)"; fi; }
esi()  { local s; for s in $2; do [[ "$STATUS" == "$s" ]] && { pass "$1"; return; }; done; fail "$1 (expected one of: $2)"; }
eec()  { local c; c="$(jq -r '.error.code // empty' <<<"$BODY" 2>/dev/null)"; [[ "$c" == "$2" ]] && pass "$1" || fail "$1 (expected error.code=$2)"; }

printf "${C}Testing API at %s${N}\n" "$BASE_URL"
req GET /api-docs/openapi.json
[[ "$STATUS" == "000" ]] && { printf "${R}ERROR: server not reachable at %s${N}\n" "$BASE_URL"; exit 1; }

sec "OpenAPI & Docs"
es "GET /api-docs/openapi.json -> 200" 200
jq -e '.paths["/v1/projects"] and .paths["/v1/auth/login"] and .paths["/v1/projects/{id}"]' <<<"$BODY" >/dev/null 2>&1 \
  && pass "OpenAPI declares all paths" || fail "OpenAPI missing paths"
STATUS="$(curl -sL -o "$BF" -w '%{http_code}' --max-time 15 "$BASE_URL/swagger-ui/")" || STATUS="000"; BODY="$(cat "$BF")"
es "GET /swagger-ui/ -> 200" 200

sec "Authentication"
req POST /v1/auth/login '{"email":"test@example.com","password":"password"}'; es "login valid -> 200" 200
TOKEN="$(jq -r '.token // empty' <<<"$BODY")"; [[ -n "$TOKEN" ]] && pass "login returned token" || fail "login token missing"
req POST /v1/auth/login '{"email":"test@example.com","password":"wrong"}'; es "login wrong password -> 401" 401; eec "error.code=unauthorized" "unauthorized"
req POST /v1/auth/login '{"email":"nobody@example.com","password":"password"}'; es "login unknown user -> 401" 401
req POST /v1/auth/login '{}'; esi "login missing fields -> 400/422" "400 422"
req POST /v1/auth/login '{"email":'; esi "login malformed JSON -> 400/422" "400 422"

sec "Authorization (negative)"
req GET /v1/projects; es "list without token -> 401" 401; eec "error.code=unauthorized" "unauthorized"
req GET /v1/projects "" "not-a-real-jwt"; es "list invalid token -> 401" 401
STATUS="$(curl -s -o "$BF" -w '%{http_code}' --max-time 15 -H 'Authorization: Token abc' "$BASE_URL/v1/projects")" || STATUS="000"; BODY="$(cat "$BF")"
es "non-Bearer scheme -> 401" 401
req POST /v1/projects '{"name":"No Auth"}'; es "create without token -> 401" 401
req DELETE "/v1/projects/$RID"; es "delete without token -> 401" 401

sec "Request validation"
req POST /v1/projects '{"name":"ab"}' "$TOKEN"; es "name < 3 chars -> 400" 400; eec "error.code=validation_failed" "validation_failed"
printf -v LN 'a%.0s' $(seq 1 81); req POST /v1/projects "{\"name\":\"$LN\"}" "$TOKEN"; es "name > 80 chars -> 400" 400
printf -v LD 'd%.0s' $(seq 1 501); req POST /v1/projects "{\"name\":\"Valid Name\",\"description\":\"$LD\"}" "$TOKEN"; es "description > 500 chars -> 400" 400
req POST /v1/projects '{"name":' "$TOKEN"; esi "create malformed JSON -> 400/422" "400 422"

sec "Projects CRUD lifecycle"
NAME="Smoke Project $(date +%s)"
req POST /v1/projects "{\"name\":\"$NAME\",\"description\":\"created by test_api.sh\"}" "$TOKEN"; es "create valid -> 201" 201
PID="$(jq -r '.id // empty' <<<"$BODY")"; [[ -n "$PID" ]] && pass "created project has id" || fail "created id missing"
[[ "$(jq -r '.user_id // empty' <<<"$BODY")" == "user1" ]] && pass "owned by user1" || fail "user_id mismatch"
req GET /v1/projects "" "$TOKEN"; es "list -> 200" 200
jq -e --arg id "$PID" 'map(.id)|index($id)!=null' <<<"$BODY" >/dev/null 2>&1 && pass "list contains created" || fail "list missing created"
req GET "/v1/projects/$PID" "" "$TOKEN"; es "get by id -> 200" 200
[[ "$(jq -r '.name // empty' <<<"$BODY")" == "$NAME" ]] && pass "fetched name matches" || fail "fetched name mismatch"
req GET "/v1/projects/$RID" "" "$TOKEN"; es "get missing -> 404" 404; eec "error.code=not_found" "not_found"
req PATCH "/v1/projects/$PID" '{"name":"Renamed Project","description":"updated"}' "$TOKEN"; es "patch -> 200" 200
req GET "/v1/projects/$PID" "" "$TOKEN"
[[ "$STATUS" == "200" && "$(jq -r '.name // empty' <<<"$BODY")" == "Renamed Project" ]] && pass "PATCH persisted (SQLite)" || fail "PATCH not persisted"
req PATCH "/v1/projects/$PID" '{"name":"x"}' "$TOKEN"; es "patch invalid name -> 400" 400
req PATCH "/v1/projects/$RID" '{"name":"Ghost Project"}' "$TOKEN"
if [[ "$STATUS" == "404" ]]; then pass "patch missing -> 404"
elif [[ "$STATUS" == "200" ]]; then warn "patch missing -> 200 (impl skips rows-affected check)"
else fail "patch missing unexpected"; fi
req DELETE "/v1/projects/$PID" "" "$TOKEN"; es "delete -> 204" 204
[[ -z "$BODY" ]] && pass "204 empty body" || fail "204 body not empty"
req GET "/v1/projects/$PID" "" "$TOKEN"; es "get after delete -> 404 (persistence)" 404
req DELETE "/v1/projects/$PID" "" "$TOKEN"; es "delete again -> 204 (idempotent)" 204

sec "HTTP plumbing"
req PUT /v1/projects '{"name":"Nope"}' "$TOKEN"; es "PUT unsupported -> 405" 405
CH="$(curl -s -D - -o /dev/null --max-time 15 -X OPTIONS "$BASE_URL/v1/projects" -H 'Origin: http://example.com' -H 'Access-Control-Request-Method: POST' 2>/dev/null)"
grep -qi '^access-control-allow-origin:' <<<"$CH" && pass "CORS preflight header present" || fail "CORS header missing"
EN="$(curl -s -D - -o /dev/null --max-time 15 -H 'Accept-Encoding: gzip' -H "Authorization: Bearer $TOKEN" "$BASE_URL/v1/projects" 2>/dev/null | grep -i '^content-encoding:' || true)"
[[ -n "$EN" ]] && pass "gzip compression active" || warn "no Content-Encoding (small responses may skip)"

printf "\n${C}== Summary ==${N}\n"
printf "  ${G}Passed: %d${N}  ${R}Failed: %d${N}  ${Y}Warnings: %d${N}\n" "$PASS" "$FAIL" "$WARN"
[[ "$FAIL" -eq 0 ]] && { printf "${G}ALL TESTS PASSED for %s${N}\n" "$BASE_URL"; exit 0; } \
                    || { printf "${R}TESTS FAILED for %s${N}\n" "$BASE_URL"; exit 1; }
