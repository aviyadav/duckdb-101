use axum::{
    Json, Router,
    extract::{Extension, Request, State},
    http::{StatusCode, header},
    middleware::Next,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use jsonwebtoken::{DecodingKey, EncodingKey, Header, Validation, decode, encode};
use serde::{Deserialize, Serialize};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use std::str::FromStr;
use std::time::Duration;
use tower_http::{compression::CompressionLayer, cors::CorsLayer, timeout::TimeoutLayer};
use utoipa::{OpenApi, ToSchema};
use utoipa_swagger_ui::SwaggerUi;
use validator::Validate;

// --- Models & OpenAPI Schemas ---

#[derive(Serialize, Deserialize, ToSchema, Clone, Debug)]
pub struct Project {
    pub id: String,
    pub name: String,
    pub description: Option<String>,
    pub user_id: String,
}

#[derive(Deserialize, ToSchema, Validate)]
pub struct CreateProjectRequest {
    #[validate(length(min = 3, max = 80))]
    pub name: String,
    #[validate(length(max = 500))]
    pub description: Option<String>,
}

#[derive(Deserialize, ToSchema)]
pub struct LoginRequest {
    pub email: String,
    pub password: String,
}

#[derive(Serialize, ToSchema)]
pub struct LoginResponse {
    pub token: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Claims {
    pub sub: String,
    pub exp: usize,
}

#[derive(Debug, Clone)]
pub struct AuthUser(pub String);

// --- Error Handling ---

#[derive(Serialize, ToSchema)]
pub struct ApiErrorBody {
    error: ErrorDetail,
}

#[derive(Serialize, ToSchema)]
pub struct ErrorDetail {
    code: String,
    message: String,
}

pub enum ApiError {
    Validation(String),
    Unauthorized,
    NotFound,
    Internal(String),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, code, message) = match self {
            ApiError::Validation(msg) => (StatusCode::BAD_REQUEST, "validation_failed", msg),
            ApiError::Unauthorized => (
                StatusCode::UNAUTHORIZED,
                "unauthorized",
                "Invalid token".into(),
            ),
            ApiError::NotFound => (
                StatusCode::NOT_FOUND,
                "not_found",
                "Resource not found".into(),
            ),
            ApiError::Internal(msg) => (StatusCode::INTERNAL_SERVER_ERROR, "internal_error", msg),
        };

        let body = Json(ApiErrorBody {
            error: ErrorDetail {
                code: code.into(),
                message,
            },
        });
        (status, body).into_response()
    }
}

// --- Database & State ---

#[derive(Clone)]
pub struct AppState {
    db: sqlx::SqlitePool,
    jwt_secret: String,
}

// --- Middleware ---

async fn auth_middleware(
    State(state): State<AppState>,
    mut req: Request,
    next: Next,
) -> Result<Response, ApiError> {
    let auth_header = req
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .ok_or(ApiError::Unauthorized)?;

    let token = auth_header
        .strip_prefix("Bearer ")
        .ok_or(ApiError::Unauthorized)?;
    let token_data = decode::<Claims>(
        token,
        &DecodingKey::from_secret(state.jwt_secret.as_bytes()),
        &Validation::default(),
    )
    .map_err(|_| ApiError::Unauthorized)?;

    req.extensions_mut().insert(AuthUser(token_data.claims.sub));
    Ok(next.run(req).await)
}

// --- Handlers ---

#[utoipa::path(post, path = "/v1/auth/login", request_body = LoginRequest, responses(
    (status = 200, description = "Login success", body = LoginResponse),
    (status = 401, description = "Unauthorized", body = ApiErrorBody)
))]
async fn login(
    State(state): State<AppState>,
    Json(req): Json<LoginRequest>,
) -> Result<Json<LoginResponse>, ApiError> {
    let user: Option<(String,)> =
        sqlx::query_as("SELECT id FROM users WHERE email = ? AND password = ?")
            .bind(&req.email)
            .bind(&req.password)
            .fetch_optional(&state.db)
            .await
            .map_err(|e| ApiError::Internal(e.to_string()))?;

    let user_id = user.ok_or(ApiError::Unauthorized)?.0;
    let claims = Claims {
        sub: user_id,
        exp: (std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 3600) as usize,
    };
    let token = encode(
        &Header::default(),
        &claims,
        &EncodingKey::from_secret(state.jwt_secret.as_bytes()),
    )
    .map_err(|e| ApiError::Internal(e.to_string()))?;

    Ok(Json(LoginResponse { token }))
}

#[utoipa::path(get, path = "/v1/projects", responses(
    (status = 200, description = "List projects", body = [Project])
))]
async fn list_projects(
    State(state): State<AppState>,
    Extension(user): Extension<AuthUser>,
) -> Result<Json<Vec<Project>>, ApiError> {
    let projects = sqlx::query_as::<_, (String, String, Option<String>, String)>(
        "SELECT id, name, description, user_id FROM projects WHERE user_id = ?",
    )
    .bind(&user.0)
    .fetch_all(&state.db)
    .await
    .map_err(|e| ApiError::Internal(e.to_string()))?;

    let res = projects
        .into_iter()
        .map(|(id, name, desc, uid)| Project {
            id,
            name,
            description: desc,
            user_id: uid,
        })
        .collect();
    Ok(Json(res))
}

#[utoipa::path(post, path = "/v1/projects", request_body = CreateProjectRequest, responses(
    (status = 201, description = "Project created", body = Project)
))]
async fn create_project(
    State(state): State<AppState>,
    Extension(user): Extension<AuthUser>,
    Json(req): Json<CreateProjectRequest>,
) -> Result<(StatusCode, Json<Project>), ApiError> {
    req.validate()
        .map_err(|e| ApiError::Validation(e.to_string()))?;
    let id = uuid::Uuid::new_v4().to_string();
    sqlx::query("INSERT INTO projects (id, name, description, user_id) VALUES (?, ?, ?, ?)")
        .bind(&id)
        .bind(&req.name)
        .bind(&req.description)
        .bind(&user.0)
        .execute(&state.db)
        .await
        .map_err(|e| ApiError::Internal(e.to_string()))?;

    Ok((
        StatusCode::CREATED,
        Json(Project {
            id,
            name: req.name,
            description: req.description,
            user_id: user.0,
        }),
    ))
}

#[utoipa::path(get, path = "/v1/projects/{id}", params(("id" = String, Path,)), responses(
    (status = 200, description = "Project found", body = Project),
    (status = 404, description = "Not found", body = ApiErrorBody)
))]
async fn get_project(
    State(state): State<AppState>,
    Extension(user): Extension<AuthUser>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> Result<Json<Project>, ApiError> {
    let proj = sqlx::query_as::<_, (String, String, Option<String>, String)>(
        "SELECT id, name, description, user_id FROM projects WHERE id = ? AND user_id = ?",
    )
    .bind(&id)
    .bind(&user.0)
    .fetch_optional(&state.db)
    .await
    .map_err(|e| ApiError::Internal(e.to_string()))?;

    let (id, name, desc, uid) = proj.ok_or(ApiError::NotFound)?;
    Ok(Json(Project {
        id,
        name,
        description: desc,
        user_id: uid,
    }))
}

#[utoipa::path(patch, path = "/v1/projects/{id}", params(("id" = String, Path,)), request_body = CreateProjectRequest, responses(
    (status = 200, description = "Project updated", body = Project)
))]
async fn update_project(
    State(state): State<AppState>,
    Extension(user): Extension<AuthUser>,
    axum::extract::Path(id): axum::extract::Path<String>,
    Json(req): Json<CreateProjectRequest>,
) -> Result<Json<Project>, ApiError> {
    req.validate()
        .map_err(|e| ApiError::Validation(e.to_string()))?;
    sqlx::query("UPDATE projects SET name = ?, description = ? WHERE id = ? AND user_id = ?")
        .bind(&req.name)
        .bind(&req.description)
        .bind(&id)
        .bind(&user.0)
        .execute(&state.db)
        .await
        .map_err(|e| ApiError::Internal(e.to_string()))?;
    Ok(Json(Project {
        id,
        name: req.name,
        description: req.description,
        user_id: user.0,
    }))
}

#[utoipa::path(delete, path = "/v1/projects/{id}", params(("id" = String, Path,)), responses(
    (status = 204, description = "Project deleted")
))]
async fn delete_project(
    State(state): State<AppState>,
    Extension(user): Extension<AuthUser>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> Result<StatusCode, ApiError> {
    sqlx::query("DELETE FROM projects WHERE id = ? AND user_id = ?")
        .bind(&id)
        .bind(&user.0)
        .execute(&state.db)
        .await
        .map_err(|e| ApiError::Internal(e.to_string()))?;
    Ok(StatusCode::NO_CONTENT)
}

// --- OpenAPI & Main ---

#[derive(OpenApi)]
#[openapi(
    paths(
        login,
        list_projects,
        create_project,
        get_project,
        update_project,
        delete_project
    ),
    components(schemas(
        Project,
        CreateProjectRequest,
        LoginRequest,
        LoginResponse,
        ApiErrorBody,
        ErrorDetail
    ))
)]
struct ApiDoc;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let opts = SqliteConnectOptions::from_str("sqlite://app.db")
        .unwrap()
        .create_if_missing(true);
    let pool = SqlitePoolOptions::new().connect_with(opts).await.unwrap();

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS users (id TEXT PRIMARY KEY, email TEXT UNIQUE, password TEXT)",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query("CREATE TABLE IF NOT EXISTS projects (id TEXT PRIMARY KEY, name TEXT, description TEXT, user_id TEXT)")
        .execute(&pool).await.unwrap();

    let _ = sqlx::query("INSERT OR IGNORE INTO users (id, email, password) VALUES ('user1', 'test@example.com', 'password')")
        .execute(&pool).await;

    let state = AppState {
        db: pool,
        jwt_secret: "super_secret_key".into(),
    };

    let protected_routes = Router::new()
        .route("/projects", get(list_projects).post(create_project))
        .route(
            "/projects/:id",
            get(get_project)
                .patch(update_project)
                .delete(delete_project),
        )
        .route_layer(axum::middleware::from_fn_with_state(
            state.clone(),
            auth_middleware,
        ));

    let app = Router::new()
        .route("/v1/auth/login", post(login))
        .nest("/v1", protected_routes)
        .merge(SwaggerUi::new("/swagger-ui").url("/api-docs/openapi.json", ApiDoc::openapi()))
        .layer(TimeoutLayer::new(Duration::from_secs(15)))
        .layer(CompressionLayer::new())
        .layer(CorsLayer::permissive())
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    println!("Axum server running on http://localhost:3000");
    println!("Swagger UI: http://localhost:3000/swagger-ui");
    axum::serve(listener, app).await.unwrap();
}
