
#!/usr/bin/env bash
# Start the Axum API (port 3000). Swagger UI: http://localhost:3000/swagger-ui/
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "${AXUM_DIR:-$SCRIPT_DIR/api-axum}"
export RUST_LOG="${RUST_LOG:-info,sqlx=warn}"
echo "==> Starting Axum API on http://localhost:3000  (docs: /swagger-ui/)"
exec cargo run --release
