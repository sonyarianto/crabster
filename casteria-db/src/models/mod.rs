use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Account {
    pub id: String,
    pub name: String,
    pub email: String,
    pub plan: AccountPlan,
    pub max_sources: i64,
    pub max_listeners: i64,
    pub max_bitrate: i64,
    pub created_at: DateTime<Utc>,
    pub active: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AccountPlan {
    Free,
    Pro,
    Enterprise,
}

impl AccountPlan {
    pub fn default_limits(&self) -> (i64, i64, i64) {
        match self {
            Self::Free => (1, 25, 128),
            Self::Pro => (5, 500, 320),
            Self::Enterprise => (50, 10000, 320),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct User {
    pub id: String,
    pub account_id: String,
    pub username: String,
    pub password_hash: String,
    pub role: UserRole,
    pub created_at: DateTime<Utc>,
    pub active: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum UserRole {
    Admin,
    Operator,
    Listener,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Station {
    pub id: String,
    pub account_id: String,
    pub name: String,
    pub description: Option<String>,
    pub genre: Option<String>,
    pub website: Option<String>,
    pub created_at: DateTime<Utc>,
    pub active: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MountConfig {
    pub id: String,
    pub station_id: String,
    pub account_id: String,
    pub mount_name: String,
    pub source_password: String,
    pub max_listeners: Option<i64>,
    pub bitrate: Option<i64>,
    pub format: Option<String>,
    pub public: bool,
    pub hidden: bool,
    pub fallback_mount: Option<String>,
    pub fallback_when_full: bool,
    pub fallback_override: bool,
    pub created_at: DateTime<Utc>,
    pub active: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiToken {
    pub id: String,
    pub user_id: String,
    pub token: String,
    pub expires_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct JwtClaims {
    pub sub: String,
    pub account_id: String,
    pub username: String,
    pub role: String,
    pub exp: usize,
    pub iat: usize,
}
