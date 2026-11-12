use actix_cors::Cors;
use actix_web::{
    App, FromRequest, HttpRequest, HttpResponse, HttpServer, Responder, middleware, web,
};
use jsonwebtoken::{DecodingKey, EncodingKey, Header, Validation, decode, encode};
use serde::{Deserialize, Serialize};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use std::str::FromStr;
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

#[derive(Debug, Serialize, ToSchema)]
pub struct ApiErrorBody {
    error: ErrorDetail,
}
#[derive(Debug, Serialize, ToSchema)]
pub struct ErrorDetail {
    code: String,
    message: String,
}

#[derive(Debug)]
pub enum ApiError {
    Validation(String),
    Unauthorized,
    NotFound,
    Internal(String),
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ApiError::Validation(msg) => write!(f, "Validation failed: {}", msg),
            ApiError::Unauthorized => write!(f, "Unauthorized"),
            ApiError::NotFound => write!(f, "Not found"),
            ApiError::Internal(msg) => write!(f, "Internal error: {}", msg),
        }
    }
}

impl actix_web::ResponseError for ApiError {
    fn error_response(&self) -> HttpResponse {
        let (status, code, message) = match self {
            ApiError::Validation(msg) => (
                actix_web::http::StatusCode::BAD_REQUEST,
                "validation_failed",
                msg.as_str(),
            ),
            ApiError::Unauthorized => (
                actix_web::http::StatusCode::UNAUTHORIZED,
                "unauthorized",
                "Invalid token",
            ),
            ApiError::NotFound => (
                actix_web::http::StatusCode::NOT_FOUND,
                "not_found",
                "Resource not found",
            ),
            ApiError::Internal(msg) => (
                actix_web::http::StatusCode::INTERNAL_SERVER_ERROR,
                "internal_error",
                msg.as_str(),
            ),
        };
        HttpResponse::build(status).json(ApiErrorBody {
            error: ErrorDetail {
                code: code.into(),
                message: message.into(),
            },
        })
    }
}

// --- Database & State ---

pub struct AppState {
    db: sqlx::SqlitePool,
    jwt_secret: String,
}

// --- Actix Extractors ---

impl FromRequest for AuthUser {
    type Error = ApiError;
    type Future = std::future::Ready<Result<Self, Self::Error>>;

    fn from_request(req: &HttpRequest, _payload: &mut actix_web::dev::Payload) -> Self::Future {
        let state = req.app_data::<web::Data<AppState>>().unwrap();
        let auth_header = req
            .headers()
            .get("Authorization")
            .and_then(|v| v.to_str().ok());

        let result = match auth_header {
            Some(h) if h.starts_with("Bearer ") => {
                let token = &h[7..];
                match decode::<Claims>(
                    token,
                    &DecodingKey::from_secret(state.jwt_secret.as_bytes()),
                    &Validation::default(),
                ) {
                    Ok(data) => Ok(AuthUser(data.claims.sub)),
                    Err(_) => Err(ApiError::Unauthorized),
                }
            }
            _ => Err(ApiError::Unauthorized),
        };
        std::future::ready(result)
    }
}

// --- Handlers ---

#[utoipa::path(post, path = "/v1/auth/login", request_body = LoginRequest, responses((status = 200, body = LoginResponse)))]
async fn login(
    state: web::Data<AppState>,
    req: web::Json<LoginRequest>,
) -> Result<impl Responder, ApiError> {
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
    Ok(HttpResponse::Ok().json(LoginResponse { token }))
}

#[utoipa::path(get, path = "/v1/projects", responses((status = 200, body = [Project])))]
async fn list_projects(
    state: web::Data<AppState>,
    user: AuthUser,
) -> Result<impl Responder, ApiError> {
    let projects = sqlx::query_as::<_, (String, String, Option<String>, String)>(
        "SELECT id, name, description, user_id FROM projects WHERE user_id = ?",
    )
    .bind(&user.0)
    .fetch_all(&state.db)
    .await
    .map_err(|e| ApiError::Internal(e.to_string()))?;
    let res: Vec<Project> = projects
        .into_iter()
        .map(|(id, name, desc, uid)| Project {
            id,
            name,
            description: desc,
            user_id: uid,
        })
        .collect();
    Ok(HttpResponse::Ok().json(res))
}

#[utoipa::path(post, path = "/v1/projects", request_body = CreateProjectRequest, responses((status = 201, body = Project)))]
async fn create_project(
    state: web::Data<AppState>,
    user: AuthUser,
    req: web::Json<CreateProjectRequest>,
) -> Result<impl Responder, ApiError> {
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
    Ok(HttpResponse::Created().json(Project {
        id,
        name: req.name.clone(),
        description: req.description.clone(),
        user_id: user.0,
    }))
}

#[utoipa::path(get, path = "/v1/projects/{id}", params(("id" = String, Path,)), responses((status = 200, body = Project)))]
async fn get_project(
    state: web::Data<AppState>,
    user: AuthUser,
    path: web::Path<String>,
) -> Result<impl Responder, ApiError> {
    let id = path.into_inner();
    let proj = sqlx::query_as::<_, (String, String, Option<String>, String)>(
        "SELECT id, name, description, user_id FROM projects WHERE id = ? AND user_id = ?",
    )
    .bind(&id)
    .bind(&user.0)
    .fetch_optional(&state.db)
    .await
    .map_err(|e| ApiError::Internal(e.to_string()))?;
    let (id, name, desc, uid) = proj.ok_or(ApiError::NotFound)?;
    Ok(HttpResponse::Ok().json(Project {
        id,
        name,
        description: desc,
        user_id: uid,
    }))
}

#[utoipa::path(patch, path = "/v1/projects/{id}", params(("id" = String, Path,)), request_body = CreateProjectRequest, responses((status = 200, body = Project)))]
async fn update_project(
    state: web::Data<AppState>,
    user: AuthUser,
    path: web::Path<String>,
    req: web::Json<CreateProjectRequest>,
) -> Result<impl Responder, ApiError> {
    req.validate()
        .map_err(|e| ApiError::Validation(e.to_string()))?;
    let id = path.into_inner();
    sqlx::query("UPDATE projects SET name = ?, description = ? WHERE id = ? AND user_id = ?")
        .bind(&req.name)
        .bind(&req.description)
        .bind(&id)
        .bind(&user.0)
        .execute(&state.db)
        .await
        .map_err(|e| ApiError::Internal(e.to_string()))?;
    Ok(HttpResponse::Ok().json(Project {
        id,
        name: req.name.clone(),
        description: req.description.clone(),
        user_id: user.0,
    }))
}

#[utoipa::path(delete, path = "/v1/projects/{id}", params(("id" = String, Path,)), responses((status = 204)))]
async fn delete_project(
    state: web::Data<AppState>,
    user: AuthUser,
    path: web::Path<String>,
) -> Result<impl Responder, ApiError> {
    let id = path.into_inner();
    sqlx::query("DELETE FROM projects WHERE id = ? AND user_id = ?")
        .bind(&id)
        .bind(&user.0)
        .execute(&state.db)
        .await
        .map_err(|e| ApiError::Internal(e.to_string()))?;
    Ok(HttpResponse::NoContent())
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

#[actix_web::main]
async fn main() -> std::io::Result<()> {
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
    sqlx::query("CREATE TABLE IF NOT EXISTS projects (id TEXT PRIMARY KEY, name TEXT, description TEXT, user_id TEXT)").execute(&pool).await.unwrap();
    let _ = sqlx::query("INSERT OR IGNORE INTO users (id, email, password) VALUES ('user1', 'test@example.com', 'password')").execute(&pool).await;

    let state = web::Data::new(AppState {
        db: pool,
        jwt_secret: "super_secret_key".into(),
    });

    println!("Actix server running on http://localhost:3001");
    println!("Swagger UI: http://localhost:3001/swagger-ui");

    HttpServer::new(move || {
        let cors = Cors::permissive();
        App::new()
            .app_data(state.clone())
            .wrap(cors)
            .wrap(middleware::Compress::default())
            .wrap(middleware::Logger::default())
            .service(
                web::scope("/v1")
                    .route("/auth/login", web::post().to(login))
                    .service(
                        web::resource("/projects")
                            .route(web::get().to(list_projects))
                            .route(web::post().to(create_project)),
                    )
                    .service(
                        web::resource("/projects/{id}")
                            .route(web::get().to(get_project))
                            .route(web::patch().to(update_project))
                            .route(web::delete().to(delete_project)),
                    ),
            )
            // Redirect /swagger-ui to /swagger-ui/ (the SwaggerUi route needs the trailing slash)
            .route(
                "/swagger-ui",
                web::get().to(|| async {
                    HttpResponse::Found()
                        .append_header(("Location", "/swagger-ui/"))
                        .finish()
                }),
            )
            .service(
                SwaggerUi::new("/swagger-ui/{_:.*}")
                    .url("/api-docs/openapi.json", ApiDoc::openapi()),
            )
    })
    .bind("0.0.0.0:3001")?
    .run()
    .await
}
