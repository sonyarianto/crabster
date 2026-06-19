use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use casteria_db::models::{Account, AccountPlan};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::SharedApiState;

#[derive(Debug, Deserialize)]
pub struct CreateAccountRequest {
    pub name: String,
    pub email: String,
    pub plan: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct AccountResponse {
    pub id: String,
    pub name: String,
    pub email: String,
    pub plan: String,
    pub max_sources: i64,
    pub max_listeners: i64,
    pub max_bitrate: i64,
    pub created_at: String,
    pub active: bool,
}

impl From<Account> for AccountResponse {
    fn from(a: Account) -> Self {
        Self {
            id: a.id,
            name: a.name,
            email: a.email,
            plan: format!("{:?}", a.plan).to_lowercase(),
            max_sources: a.max_sources,
            max_listeners: a.max_listeners,
            max_bitrate: a.max_bitrate,
            created_at: a.created_at.to_rfc3339(),
            active: a.active,
        }
    }
}

pub async fn list_accounts(
    State(state): State<SharedApiState>,
) -> Result<Json<Vec<AccountResponse>>, (StatusCode, Json<Value>)> {
    let db = state.db.as_ref().ok_or_else(|| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "database not available"})),
        )
    })?;

    match db.list_accounts() {
        Ok(accounts) => {
            let resp: Vec<AccountResponse> = accounts.into_iter().map(Into::into).collect();
            Ok(Json(resp))
        }
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        )),
    }
}

pub async fn create_account(
    State(state): State<SharedApiState>,
    Json(req): Json<CreateAccountRequest>,
) -> Result<Json<AccountResponse>, (StatusCode, Json<Value>)> {
    let db = state.db.as_ref().ok_or_else(|| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "database not available"})),
        )
    })?;

    let plan = match req.plan.as_deref().unwrap_or("free") {
        "pro" => AccountPlan::Pro,
        "enterprise" => AccountPlan::Enterprise,
        _ => AccountPlan::Free,
    };

    match db.create_account(&req.name, &req.email, plan) {
        Ok(account) => Ok(Json(AccountResponse::from(account))),
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        )),
    }
}

pub async fn get_account(
    State(state): State<SharedApiState>,
    Path(id): Path<String>,
) -> Result<Json<AccountResponse>, (StatusCode, Json<Value>)> {
    let db = state.db.as_ref().ok_or_else(|| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "database not available"})),
        )
    })?;

    match db.get_account(&id) {
        Ok(Some(account)) => Ok(Json(AccountResponse::from(account))),
        Ok(None) => Err((
            StatusCode::NOT_FOUND,
            Json(json!({"error": "account not found"})),
        )),
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        )),
    }
}
