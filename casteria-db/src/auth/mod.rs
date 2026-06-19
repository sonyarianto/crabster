use std::time::{SystemTime, UNIX_EPOCH};

use jsonwebtoken::{decode, encode, DecodingKey, EncodingKey, Header, Validation};
use uuid::Uuid;

use crate::models::{JwtClaims, User, UserRole};
use crate::Database;

const JWT_SECRET: &str = "casteria-jwt-secret-change-me-in-production";

pub fn hash_password(password: &str) -> Result<String, anyhow::Error> {
    Ok(bcrypt::hash(password, bcrypt::DEFAULT_COST)?)
}

pub fn verify_password(password: &str, hash: &str) -> Result<bool, anyhow::Error> {
    Ok(bcrypt::verify(password, hash)?)
}

pub fn create_jwt(user: &User, account_id: &str) -> Result<String, anyhow::Error> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as usize;

    let claims = JwtClaims {
        sub: user.id.clone(),
        account_id: account_id.to_string(),
        username: user.username.clone(),
        role: format!("{:?}", user.role).to_lowercase(),
        exp: now + 86400,
        iat: now,
    };

    Ok(encode(
        &Header::default(),
        &claims,
        &EncodingKey::from_secret(JWT_SECRET.as_ref()),
    )?)
}

pub fn verify_jwt(token: &str) -> Result<JwtClaims, anyhow::Error> {
    let data = decode::<JwtClaims>(
        token,
        &DecodingKey::from_secret(JWT_SECRET.as_ref()),
        &Validation::default(),
    )?;
    Ok(data.claims)
}

pub fn authenticate(db: &Database, username: &str, password: &str) -> Result<(User, String, String), anyhow::Error> {
    let conn = db.conn();

    let mut stmt = conn.prepare(
        "SELECT u.id, u.account_id, u.username, u.password_hash, u.role, u.created_at, u.active
         FROM users u WHERE u.username = ?1 AND u.active = 1",
    )?;

    let user = stmt.query_row(rusqlite::params![username], |row| {
        let id: String = row.get(0)?;
        let account_id: String = row.get(1)?;
        let username: String = row.get(2)?;
        let password_hash: String = row.get(3)?;
        let role_str: String = row.get(4)?;
        let created_at_str: String = row.get(5)?;
        let active: bool = row.get::<_, i32>(6)? != 0;

        let role = match role_str.to_lowercase().as_str() {
            "admin" => UserRole::Admin,
            "operator" => UserRole::Operator,
            _ => UserRole::Listener,
        };

        Ok((
            User {
                id,
                account_id: account_id.clone(),
                username,
                password_hash,
                role,
                created_at: chrono::DateTime::parse_from_str(
                    &created_at_str,
                    "%Y-%m-%dT%H:%M:%S%.fZ",
                )
                .map(|d| d.to_utc())
                .unwrap_or_else(|_| chrono::Utc::now()),
                active,
            },
            account_id,
        ))
    }).map_err(|_| anyhow::anyhow!("invalid credentials"))?;

    let (user, account_id) = user;

    if !verify_password(password, &user.password_hash)? {
        return Err(anyhow::anyhow!("invalid credentials"));
    }

    let token = create_jwt(&user, &account_id)?;

    Ok((user, account_id, token))
}

pub fn register_default_admin(db: &Database) -> Result<(), anyhow::Error> {
    let conn = db.conn();

    let count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM accounts",
        [],
        |row| row.get(0),
    )?;

    if count > 0 {
        return Ok(());
    }

    let account_id = Uuid::new_v4().to_string();
    let user_id = Uuid::new_v4().to_string();
    let pw_hash = hash_password("admin")?;

    conn.execute(
        "INSERT INTO accounts (id, name, email, plan, max_sources, max_listeners, max_bitrate)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        rusqlite::params![account_id, "Default Account", "admin@casteria.local", "enterprise", 50, 10000, 320],
    )?;

    conn.execute(
        "INSERT INTO users (id, account_id, username, password_hash, role)
         VALUES (?1, ?2, ?3, ?4, ?5)",
        rusqlite::params![user_id, account_id, "admin", pw_hash, "admin"],
    )?;

    tracing::info!("Created default admin account (user: admin, password: admin)");
    Ok(())
}
