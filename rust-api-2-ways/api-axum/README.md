# api-axum

A small REST API built with [Axum](https://github.com/tokio-rs/axum), demonstrating JWT authentication, SQLite persistence with SQLx, request validation, and OpenAPI docs via utoipa + Swagger UI.

## Tech Stack

- **Axum 0.7** — web framework
- **SQLx (SQLite)** — database (auto-creates `app.db` on first run)
- **jsonwebtoken** — JWT auth (tokens expire after 1 hour)
- **validator** — request body validation
- **utoipa / utoipa-swagger-ui** — OpenAPI docs
- **tower-http** — CORS, gzip compression, and request timeout (15s) middleware

## Endpoints

| Method | Path                | Auth | Description                          |
| ------ | ------------------- | ---- | ------------------------------------ |
| POST   | `/v1/auth/login`    | No   | Login, returns a JWT                 |
| GET    | `/v1/projects`      | Yes  | List your projects                   |
| POST   | `/v1/projects`      | Yes  | Create a project                     |
| GET    | `/v1/projects/:id`  | Yes  | Get a single project                 |
| PATCH  | `/v1/projects/:id`  | Yes  | Update a project                     |
| DELETE | `/v1/projects/:id`  | Yes  | Delete a project (returns 204)       |

Authenticated endpoints require an `Authorization: Bearer <token>` header.

## How to Run

```bash
cargo run
```

Server starts on http://localhost:3000

A test user is seeded automatically on startup:

- **Email:** `test@example.com`
- **Password:** `password`

## Test the API

**Get a token:**

```bash
curl -X POST http://localhost:3000/v1/auth/login \
  -H "Content-Type: application/json" \
  -d '{"email":"test@example.com","password":"password"}'
```

**Create a project** (use the token from the previous step):

```bash
curl -X POST http://localhost:3000/v1/projects \
  -H "Authorization: Bearer <YOUR_TOKEN>" \
  -H "Content-Type: application/json" \
  -d '{"name":"My Project","description":"Testing SQLite"}'
```

Project names must be 3–80 characters; descriptions are optional (max 500 characters).

**List projects:**

```bash
curl http://localhost:3000/v1/projects \
  -H "Authorization: Bearer <YOUR_TOKEN>"
```

**Get / Update / Delete a project:**

```bash
curl http://localhost:3000/v1/projects/<PROJECT_ID> \
  -H "Authorization: Bearer <YOUR_TOKEN>"

curl -X PATCH http://localhost:3000/v1/projects/<PROJECT_ID> \
  -H "Authorization: Bearer <YOUR_TOKEN>" \
  -H "Content-Type: application/json" \
  -d '{"name":"Renamed Project","description":"Updated"}'

curl -X DELETE http://localhost:3000/v1/projects/<PROJECT_ID> \
  -H "Authorization: Bearer <YOUR_TOKEN>"
```

## Scripts

Helper scripts are in `scripts/`:

- `scripts/run.sh` — start the server (`cargo run`)
- `scripts/test-all.sh` — run an end-to-end test suite against all endpoints (expects the server to be running on `http://localhost:3000`, override with `BASE_URL`)
- `scripts/login.sh` — log in and print the token JSON
- `scripts/list-projects.sh` — list projects (`TOKEN` env var required)
- `scripts/create-project.sh [name] [description]` — create a project (`TOKEN` env var required)
- `scripts/get-project.sh <project_id>` — fetch a project (`TOKEN` env var required)
- `scripts/update-project.sh <project_id> [name] [description]` — update a project (`TOKEN` env var required)
- `scripts/delete-project.sh <project_id>` — delete a project (`TOKEN` env var required)

## OpenAPI Docs

Navigate to http://localhost:3000/swagger-ui to interact with the endpoints via the Swagger UI. The raw spec is served at http://localhost:3000/api-docs/openapi.json.
