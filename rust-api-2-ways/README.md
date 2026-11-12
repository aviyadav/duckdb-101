## All scripts are written and verified end-to-end. The verification run confirmed every behavior works correctly:

- Python suite vs. a healthy API → exit 0, 33 passed / 0 failed / 1 warning
- test_all.sh (both APIs healthy) → exit 0, "BOTH APIs passed"
- test_all.sh (one API broken) → exit 1, correctly flagged 33 failures
- Dependency guard (missing jq) → exit 1 with install hint
- Unreachable server → exit 2 with a friendly message


Below are the five scripts. Drop them into your workspace root (next to api-axum/ and api-actix-web/).

```text
workspace/
├── api-axum/            # your Axum project
├── api-actix-web/       # your Actix project
├── run_axum.sh          # start Axum   (:3000)
├── run_actix.sh         # start Actix  (:3001)
├── test_api.py          # full test suite (Python stdlib only — no installs)
├── test_api.sh          # same suite in bash (needs curl + jq)
└── test_all.sh          # build → start → test BOTH → stop, one command
```


## How to use

```bash
chmod +x run_axum.sh run_actix.sh test_api.sh test_all.sh test_api.py

# Option A — fully automatic (builds, starts, tests BOTH, then stops them):
./test_all.sh

# Option B — run servers yourself, then test:
./run_axum.sh                 # terminal 1  (:3000)
./run_actix.sh                # terminal 2  (:3001)
python3 test_api.py           # test Axum      (no dependencies)
python3 test_api.py http://localhost:3001   # test Actix
./test_api.sh http://localhost:3000         # bash alternative (needs jq)
./test_all.sh --external      # test both already-running servers

# Custom locations / ports:
AXUM_DIR=~/code/api-axum ACTIX_URL=http://localhost:4001 ./test_all.sh --external
```

## What gets tested (per API, 33 checks)

| Area | Checks |
| --- | --- |
| OpenAPI/Docs | spec serves all paths; Swagger UI loads |
| Auth | valid login + token; wrong password / unknown user → 401; missing/malformed body |
| AuthZ | no token, garbage token, non-Bearer, **expired JWT, wrong-secret JWT** → 401 |
| Validation | name <3 / >80, description >500, malformed JSON → 400 |
| CRUD + SQLite | create→list→get→patch→**verify persisted**→delete→**404 after delete**→idempotent delete |
| Plumbing | 405 on PUT, CORS preflight, gzip, oversized-body rejection |
