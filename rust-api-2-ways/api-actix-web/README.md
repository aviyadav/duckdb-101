# api-actix-web

A small REST API built with Actix Web, SQLite (sqlx), JWT auth, and OpenAPI docs via utoipa + Swagger UI.

## How to Run

```bash
cargo run
```

Server starts on http://localhost:3001

A SQLite database (`app.db`) is created automatically on first run, along with a seeded test user:

- Email: `test@example.com`
- Password: `password`

## Test the API

* **Get Token**

```bash
curl -X POST http://localhost:3001/v1/auth/login \
-H "Content-Type: application/json" \
-d '{"email":"test@example.com","password":"password"}'
```

* **Create Project (use the token from the previous step)**

```bash
curl -X POST http://localhost:3001/v1/projects \
-H "Authorization: Bearer <YOUR_TOKEN>" \
-H "Content-Type: application/json" \
-d '{"name":"My Project","description":"Testing SQLite"}'
```

* **List Projects**

```bash
curl http://localhost:3001/v1/projects \
-H "Authorization: Bearer <YOUR_TOKEN>"
```

## Endpoints

| Method | Path                 | Description              | Auth     |
|--------|----------------------|--------------------------|----------|
| POST   | `/v1/auth/login`     | Login, returns JWT token | No       |
| GET    | `/v1/projects`       | List your projects       | Bearer   |
| POST   | `/v1/projects`       | Create a project         | Bearer   |
| GET    | `/v1/projects/{id}`  | Get a project            | Bearer   |
| PATCH  | `/v1/projects/{id}`  | Update a project         | Bearer   |
| DELETE | `/v1/projects/{id}`  | Delete a project         | Bearer   |

## OpenAPI Docs

Navigate to http://localhost:3001/swagger-ui to interact with the endpoints via the Swagger UI (it redirects to `/swagger-ui/`).

The raw OpenAPI spec is served at http://localhost:3001/api-docs/openapi.json.
