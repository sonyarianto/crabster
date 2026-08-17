use uuid::Uuid;

use crate::auth::hash_password;
use crate::models::*;
use crate::Database;

impl Database {
    // ── Account operations ──

    pub fn create_account(
        &self,
        name: &str,
        email: &str,
        plan: AccountPlan,
    ) -> Result<Account, anyhow::Error> {
        let id = Uuid::new_v4().to_string();
        let (sources, listeners, bitrate) = plan.default_limits();

        let conn = self.conn();
        conn.execute(
            "INSERT INTO accounts (id, name, email, plan, max_sources, max_listeners, max_bitrate)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            rusqlite::params![
                id,
                name,
                email,
                format!("{:?}", plan).to_lowercase(),
                sources,
                listeners,
                bitrate
            ],
        )?;

        Ok(Account {
            id,
            name: name.to_string(),
            email: email.to_string(),
            plan,
            max_sources: sources,
            max_listeners: listeners,
            max_bitrate: bitrate,
            created_at: chrono::Utc::now(),
            active: true,
        })
    }

    pub fn get_account(&self, id: &str) -> Result<Option<Account>, anyhow::Error> {
        let conn = self.conn();
        let mut stmt = conn.prepare(
            "SELECT id, name, email, plan, max_sources, max_listeners, max_bitrate, created_at, active
             FROM accounts WHERE id = ?1",
        )?;

        let mut rows = stmt.query_map(rusqlite::params![id], |row| {
            let plan_str: String = row.get(3)?;
            let created_at_str: String = row.get(7)?;
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                plan_str,
                row.get::<_, i64>(4)?,
                row.get::<_, i64>(5)?,
                row.get::<_, i64>(6)?,
                created_at_str,
                row.get::<_, i32>(8)? != 0,
            ))
        })?;

        match rows.next() {
            Some(Ok((id, name, email, plan_str, ms, ml, mb, created_at_str, active))) => {
                let plan = match plan_str.to_lowercase().as_str() {
                    "pro" => AccountPlan::Pro,
                    "enterprise" => AccountPlan::Enterprise,
                    _ => AccountPlan::Free,
                };
                Ok(Some(Account {
                    id,
                    name,
                    email,
                    plan,
                    max_sources: ms,
                    max_listeners: ml,
                    max_bitrate: mb,
                    created_at: chrono::DateTime::parse_from_str(
                        &created_at_str,
                        "%Y-%m-%dT%H:%M:%S%.fZ",
                    )
                    .map(|d| d.to_utc())
                    .unwrap_or_else(|_| chrono::Utc::now()),
                    active,
                }))
            }
            _ => Ok(None),
        }
    }

    pub fn list_accounts(&self) -> Result<Vec<Account>, anyhow::Error> {
        let conn = self.conn();
        let mut stmt = conn.prepare(
            "SELECT id, name, email, plan, max_sources, max_listeners, max_bitrate, created_at, active
             FROM accounts ORDER BY created_at DESC",
        )?;

        let accounts = stmt
            .query_map([], |row| {
                let plan_str: String = row.get(3)?;
                let created_at_str: String = row.get(7)?;
                let plan = match plan_str.to_lowercase().as_str() {
                    "pro" => AccountPlan::Pro,
                    "enterprise" => AccountPlan::Enterprise,
                    _ => AccountPlan::Free,
                };
                Ok(Account {
                    id: row.get(0)?,
                    name: row.get(1)?,
                    email: row.get(2)?,
                    plan,
                    max_sources: row.get(4)?,
                    max_listeners: row.get(5)?,
                    max_bitrate: row.get(6)?,
                    created_at: chrono::DateTime::parse_from_str(
                        &created_at_str,
                        "%Y-%m-%dT%H:%M:%S%.fZ",
                    )
                    .map(|d| d.to_utc())
                    .unwrap_or_else(|_| chrono::Utc::now()),
                    active: row.get::<_, i32>(8)? != 0,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;

        Ok(accounts)
    }

    // ── User operations ──

    pub fn create_user(
        &self,
        account_id: &str,
        username: &str,
        password: &str,
        role: UserRole,
    ) -> Result<User, anyhow::Error> {
        let id = Uuid::new_v4().to_string();
        let pw_hash = hash_password(password)?;

        let conn = self.conn();
        conn.execute(
            "INSERT INTO users (id, account_id, username, password_hash, role)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params![
                id,
                account_id,
                username,
                pw_hash,
                format!("{:?}", role).to_lowercase()
            ],
        )?;

        Ok(User {
            id,
            account_id: account_id.to_string(),
            username: username.to_string(),
            password_hash: pw_hash,
            role,
            created_at: chrono::Utc::now(),
            active: true,
        })
    }

    pub fn get_user_by_username(&self, username: &str) -> Result<Option<User>, anyhow::Error> {
        let conn = self.conn();
        let mut stmt = conn.prepare(
            "SELECT id, account_id, username, password_hash, role, created_at, active
             FROM users WHERE username = ?1",
        )?;

        let mut rows = stmt.query_map(rusqlite::params![username], |row| {
            let role_str: String = row.get(4)?;
            let created_at_str: String = row.get(5)?;
            let role = match role_str.to_lowercase().as_str() {
                "admin" => UserRole::Admin,
                "operator" => UserRole::Operator,
                _ => UserRole::Listener,
            };
            Ok(User {
                id: row.get(0)?,
                account_id: row.get(1)?,
                username: row.get(2)?,
                password_hash: row.get(3)?,
                role,
                created_at: chrono::DateTime::parse_from_str(
                    &created_at_str,
                    "%Y-%m-%dT%H:%M:%S%.fZ",
                )
                .map(|d| d.to_utc())
                .unwrap_or_else(|_| chrono::Utc::now()),
                active: row.get::<_, i32>(6)? != 0,
            })
        })?;

        match rows.next() {
            Some(Ok(user)) => Ok(Some(user)),
            _ => Ok(None),
        }
    }

    pub fn list_users(&self, account_id: &str) -> Result<Vec<User>, anyhow::Error> {
        let conn = self.conn();
        let mut stmt = conn.prepare(
            "SELECT id, account_id, username, password_hash, role, created_at, active
             FROM users WHERE account_id = ?1 ORDER BY created_at DESC",
        )?;

        let users = stmt
            .query_map(rusqlite::params![account_id], |row| {
                let role_str: String = row.get(4)?;
                let created_at_str: String = row.get(5)?;
                let role = match role_str.to_lowercase().as_str() {
                    "admin" => UserRole::Admin,
                    "operator" => UserRole::Operator,
                    _ => UserRole::Listener,
                };
                Ok(User {
                    id: row.get(0)?,
                    account_id: row.get(1)?,
                    username: row.get(2)?,
                    password_hash: row.get(3)?,
                    role,
                    created_at: chrono::DateTime::parse_from_str(
                        &created_at_str,
                        "%Y-%m-%dT%H:%M:%S%.fZ",
                    )
                    .map(|d| d.to_utc())
                    .unwrap_or_else(|_| chrono::Utc::now()),
                    active: row.get::<_, i32>(6)? != 0,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;

        Ok(users)
    }

    // ── Station operations ──

    pub fn create_station(
        &self,
        account_id: &str,
        name: &str,
        description: Option<&str>,
        genre: Option<&str>,
        website: Option<&str>,
    ) -> Result<Station, anyhow::Error> {
        let id = Uuid::new_v4().to_string();

        let conn = self.conn();
        conn.execute(
            "INSERT INTO stations (id, account_id, name, description, genre, website)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            rusqlite::params![id, account_id, name, description, genre, website],
        )?;

        Ok(Station {
            id,
            account_id: account_id.to_string(),
            name: name.to_string(),
            description: description.map(String::from),
            genre: genre.map(String::from),
            website: website.map(String::from),
            created_at: chrono::Utc::now(),
            active: true,
        })
    }

    pub fn list_stations(&self, account_id: &str) -> Result<Vec<Station>, anyhow::Error> {
        let conn = self.conn();
        let mut stmt = conn.prepare(
            "SELECT id, account_id, name, description, genre, website, created_at, active
             FROM stations WHERE account_id = ?1 ORDER BY created_at DESC",
        )?;

        let stations = stmt
            .query_map(rusqlite::params![account_id], |row| {
                let created_at_str: String = row.get(6)?;
                Ok(Station {
                    id: row.get(0)?,
                    account_id: row.get(1)?,
                    name: row.get(2)?,
                    description: row.get(3)?,
                    genre: row.get(4)?,
                    website: row.get(5)?,
                    created_at: chrono::DateTime::parse_from_str(
                        &created_at_str,
                        "%Y-%m-%dT%H:%M:%S%.fZ",
                    )
                    .map(|d| d.to_utc())
                    .unwrap_or_else(|_| chrono::Utc::now()),
                    active: row.get::<_, i32>(7)? != 0,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;

        Ok(stations)
    }

    // ── Mount config operations ──

    pub fn create_mount_config(
        &self,
        station_id: &str,
        account_id: &str,
        mount_name: &str,
        source_password: &str,
        public: bool,
    ) -> Result<MountConfig, anyhow::Error> {
        let id = Uuid::new_v4().to_string();

        let conn = self.conn();
        conn.execute(
            "INSERT INTO mount_configs (id, station_id, account_id, mount_name, source_password, public)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            rusqlite::params![id, station_id, account_id, mount_name, source_password, public as i32],
        )?;

        Ok(MountConfig {
            id,
            station_id: station_id.to_string(),
            account_id: account_id.to_string(),
            mount_name: mount_name.to_string(),
            source_password: source_password.to_string(),
            max_listeners: None,
            bitrate: None,
            format: None,
            public,
            hidden: false,
            fallback_mount: None,
            fallback_when_full: false,
            fallback_override: false,
            created_at: chrono::Utc::now(),
            active: true,
        })
    }

    pub fn get_mount_config(&self, mount_name: &str) -> Result<Option<MountConfig>, anyhow::Error> {
        let conn = self.conn();
        let mut stmt = conn.prepare(
            "SELECT id, station_id, account_id, mount_name, source_password,
                    max_listeners, bitrate, format, public, hidden,
                    fallback_mount, fallback_when_full, fallback_override, created_at, active
             FROM mount_configs WHERE mount_name = ?1 AND active = 1",
        )?;

        let mut rows = stmt.query_map(rusqlite::params![mount_name], |row| {
            let created_at_str: String = row.get(13)?;
            Ok(MountConfig {
                id: row.get(0)?,
                station_id: row.get(1)?,
                account_id: row.get(2)?,
                mount_name: row.get(3)?,
                source_password: row.get(4)?,
                max_listeners: row.get(5)?,
                bitrate: row.get(6)?,
                format: row.get(7)?,
                public: row.get::<_, i32>(8)? != 0,
                hidden: row.get::<_, i32>(9)? != 0,
                fallback_mount: row.get(10)?,
                fallback_when_full: row.get::<_, i32>(11)? != 0,
                fallback_override: row.get::<_, i32>(12)? != 0,
                created_at: chrono::DateTime::parse_from_str(
                    &created_at_str,
                    "%Y-%m-%dT%H:%M:%S%.fZ",
                )
                .map(|d| d.to_utc())
                .unwrap_or_else(|_| chrono::Utc::now()),
                active: row.get::<_, i32>(14)? != 0,
            })
        })?;

        match rows.next() {
            Some(Ok(config)) => Ok(Some(config)),
            _ => Ok(None),
        }
    }

    pub fn find_mount_by_source_password(
        &self,
        source_password: &str,
    ) -> Result<Option<MountConfig>, anyhow::Error> {
        let conn = self.conn();
        let mut stmt = conn.prepare(
            "SELECT id, station_id, account_id, mount_name, source_password,
                    max_listeners, bitrate, format, public, hidden,
                    fallback_mount, fallback_when_full, fallback_override, created_at, active
             FROM mount_configs WHERE source_password = ?1 AND active = 1 LIMIT 1",
        )?;

        let mut rows = stmt.query_map(rusqlite::params![source_password], |row| {
            let created_at_str: String = row.get(13)?;
            Ok(MountConfig {
                id: row.get(0)?,
                station_id: row.get(1)?,
                account_id: row.get(2)?,
                mount_name: row.get(3)?,
                source_password: row.get(4)?,
                max_listeners: row.get(5)?,
                bitrate: row.get(6)?,
                format: row.get(7)?,
                public: row.get::<_, i32>(8)? != 0,
                hidden: row.get::<_, i32>(9)? != 0,
                fallback_mount: row.get(10)?,
                fallback_when_full: row.get::<_, i32>(11)? != 0,
                fallback_override: row.get::<_, i32>(12)? != 0,
                created_at: chrono::DateTime::parse_from_str(
                    &created_at_str,
                    "%Y-%m-%dT%H:%M:%S%.fZ",
                )
                .map(|d| d.to_utc())
                .unwrap_or_else(|_| chrono::Utc::now()),
                active: row.get::<_, i32>(14)? != 0,
            })
        })?;

        match rows.next() {
            Some(Ok(config)) => Ok(Some(config)),
            _ => Ok(None),
        }
    }

    pub fn list_mount_configs(&self, account_id: &str) -> Result<Vec<MountConfig>, anyhow::Error> {
        let conn = self.conn();
        let mut stmt = conn.prepare(
            "SELECT id, station_id, account_id, mount_name, source_password,
                    max_listeners, bitrate, format, public, hidden,
                    fallback_mount, fallback_when_full, fallback_override, created_at, active
             FROM mount_configs WHERE account_id = ?1 ORDER BY created_at DESC",
        )?;

        let configs = stmt
            .query_map(rusqlite::params![account_id], |row| {
                let created_at_str: String = row.get(13)?;
                Ok(MountConfig {
                    id: row.get(0)?,
                    station_id: row.get(1)?,
                    account_id: row.get(2)?,
                    mount_name: row.get(3)?,
                    source_password: row.get(4)?,
                    max_listeners: row.get(5)?,
                    bitrate: row.get(6)?,
                    format: row.get(7)?,
                    public: row.get::<_, i32>(8)? != 0,
                    hidden: row.get::<_, i32>(9)? != 0,
                    fallback_mount: row.get(10)?,
                    fallback_when_full: row.get::<_, i32>(11)? != 0,
                    fallback_override: row.get::<_, i32>(12)? != 0,
                    created_at: chrono::DateTime::parse_from_str(
                        &created_at_str,
                        "%Y-%m-%dT%H:%M:%S%.fZ",
                    )
                    .map(|d| d.to_utc())
                    .unwrap_or_else(|_| chrono::Utc::now()),
                    active: row.get::<_, i32>(14)? != 0,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;

        Ok(configs)
    }

    pub fn get_account_source_count(&self, account_id: &str) -> Result<i64, anyhow::Error> {
        let conn = self.conn();
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM mount_configs WHERE account_id = ?1 AND active = 1",
            rusqlite::params![account_id],
            |row| row.get(0),
        )?;
        Ok(count)
    }

    pub fn check_account_quota(&self, account_id: &str) -> Result<(bool, String), anyhow::Error> {
        let account = self
            .get_account(account_id)?
            .ok_or_else(|| anyhow::anyhow!("account not found"))?;
        let source_count = self.get_account_source_count(account_id)?;

        if source_count >= account.max_sources {
            return Ok((
                false,
                format!(
                    "source limit reached ({}/{}). Upgrade plan.",
                    source_count, account.max_sources
                ),
            ));
        }

        Ok((true, String::new()))
    }
}
