#!/usr/bin/env bash
# test_all.sh - Build, start, test and stop BOTH APIs automatically.
#   ./test_all.sh              full cycle: build -> start -> test -> stop
#   ./test_all.sh --external   test already-running servers (no build/start)
# Env overrides: AXUM_DIR, ACTIX_DIR, AXUM_URL, ACTIX_URL
set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
AXUM_DIR="${AXUM_DIR:-$SCRIPT_DIR/api-axum}"
ACTIX_DIR="${ACTIX_DIR:-$SCRIPT_DIR/api-actix-web}"
AXUM_URL="${AXUM_URL:-http://localhost:3000}"
ACTIX_URL="${ACTIX_URL:-http://localhost:3001}"
EXTERNAL=0; [[ "${1:-}" == "--external" ]] && EXTERNAL=1
command -v curl >/dev/null 2>&1 || { echo "ERROR: curl is required"; exit 1; }

PIDS=()
cleanup() { for pid in "${PIDS[@]:-}"; do [[ -n "$pid" ]] && kill "$pid" 2>/dev/null; done; }
trap cleanup EXIT INT TERM

resolve_bin() { # dir -> release binary name (handles api-actix vs api-actix-web)
  local dir="$1" name="" meta
  meta="$(cd "$dir" && cargo metadata --no-deps --format-version 1 2>/dev/null)" || meta=""
  if [[ -n "$meta" ]] && command -v python3 >/dev/null 2>&1; then
    name="$(printf '%s' "$meta" | python3 -c 'import json,sys
try:
    m=json.load(sys.stdin); print(next(t["name"] for p in m["packages"] for t in p["targets"] if "bin" in t["kind"]))
except Exception: pass')"
  elif [[ -n "$meta" ]] && command -v jq >/dev/null 2>&1; then
    name="$(printf '%s' "$meta" | jq -r '[.packages[].targets[] | select(.kind[]=="bin") | .name][0] // empty')"
  fi
  if [[ -n "$name" ]]; then printf '%s\n' "$name"; else basename "$dir"; fi
}

wait_ready() { # url name [pid] [logfile]
  local url="$1" name="$2" pid="${3:-}" log="${4:-}" i
  for i in $(seq 1 60); do
    curl -sf -o /dev/null --max-time 2 "$url/api-docs/openapi.json" && { echo "==> $name is ready at $url"; return 0; }
    if [[ -n "$pid" ]] && ! kill -0 "$pid" 2>/dev/null; then
      echo "ERROR: $name process died. Last log lines:"; [[ -n "$log" && -f "$log" ]] && tail -n 20 "$log"; return 1
    fi
    sleep 1
  done
  echo "ERROR: $name did not become ready within 60s"; [[ -n "$log" && -f "$log" ]] && tail -n 20 "$log"; return 1
}

if [[ "$EXTERNAL" -eq 0 ]]; then
  echo "==> Building Axum API ($AXUM_DIR)";  (cd "$AXUM_DIR"  && cargo build --release) || exit 1
  echo "==> Building Actix Web API ($ACTIX_DIR)"; (cd "$ACTIX_DIR" && cargo build --release) || exit 1
  AB="$(resolve_bin "$AXUM_DIR")"; CB="$(resolve_bin "$ACTIX_DIR")"
  (cd "$AXUM_DIR"  && exec "./target/release/$AB") > "$SCRIPT_DIR/axum-server.log"  2>&1 & PIDS+=($!)
  (cd "$ACTIX_DIR" && exec "./target/release/$CB") > "$SCRIPT_DIR/actix-server.log" 2>&1 & PIDS+=($!)
  wait_ready "$AXUM_URL"  "Axum"      "${PIDS[0]}" "$SCRIPT_DIR/axum-server.log"  || exit 1
  wait_ready "$ACTIX_URL" "Actix Web" "${PIDS[1]}" "$SCRIPT_DIR/actix-server.log" || exit 1
else
  wait_ready "$AXUM_URL"  "Axum"      || exit 1
  wait_ready "$ACTIX_URL" "Actix Web" || exit 1
fi

run_suite() { # url label
  echo; echo "############################################################"
  echo "# Testing $2  ($1)"; echo "############################################################"
  if command -v python3 >/dev/null 2>&1; then python3 "$SCRIPT_DIR/test_api.py" "$1"
  else bash "$SCRIPT_DIR/test_api.sh" "$1"; fi
}

FAILED=0
run_suite "$AXUM_URL"  "Axum"      || FAILED=$((FAILED+1))
run_suite "$ACTIX_URL" "Actix Web" || FAILED=$((FAILED+1))
echo
if [[ "$FAILED" -eq 0 ]]; then echo "RESULT: BOTH APIs passed the full test suite."
else echo "RESULT: $FAILED API(s) failed the test suite."; fi
[[ "$EXTERNAL" -eq 0 ]] && echo "Server logs: $SCRIPT_DIR/axum-server.log, $SCRIPT_DIR/actix-server.log"
exit "$FAILED"
