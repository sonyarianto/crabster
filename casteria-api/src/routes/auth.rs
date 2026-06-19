use axum::{extract::State, http::StatusCode, Json};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::SharedApiState;

#[derive(Debug, Deserialize)]
pub struct LoginRequest {
    pub username: String,
    pub password: String,
}

#[derive(Debug, Serialize)]
pub struct LoginResponse {
    pub token: String,
    pub user_id: String,
    pub account_id: String,
    pub username: String,
    pub role: String,
}

pub async fn login(
    State(state): State<SharedApiState>,
    Json(req): Json<LoginRequest>,
) -> Result<Json<LoginResponse>, (StatusCode, Json<Value>)> {
    let db = state
        .db
        .as_ref()
        .ok_or_else(|| {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": "database not available"})),
            )
        })?;

    match casteria_db::auth::authenticate(db, &req.username, &req.password) {
        Ok((user, account_id, token)) => Ok(Json(LoginResponse {
            token,
            user_id: user.id.clone(),
            account_id,
            username: user.username,
            role: format!("{:?}", user.role).to_lowercase(),
        })),
        Err(_) => Err((
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": "invalid credentials"})),
        )),
    }
}

pub async fn verify(State(_state): State<SharedApiState>) -> Json<Value> {
    Json(json!({"valid": true}))
}
