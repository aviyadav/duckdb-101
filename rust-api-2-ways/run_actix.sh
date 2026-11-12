#!/usr/bin/env bash
# Start the Actix Web API (port 3001). Swagger UI: http://localhost:3001/swagger-ui/
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "${ACTIX_DIR:-$SCRIPT_DIR/api-actix-web}"
export RUST_LOG="${RUST_LOG:-info,sqlx=warn}"
echo "==> Starting Actix Web API on http://localhost:3001  (docs: /swagger-ui/)"
exec cargo run --release
