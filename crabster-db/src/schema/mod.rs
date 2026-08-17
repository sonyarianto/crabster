use crate::Database;
use tracing::info;

pub fn initialize(db: &Database) -> Result<(), anyhow::Error> {
    let conn = db.conn();

    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS accounts (
            id          TEXT PRIMARY KEY,
            name        TEXT NOT NULL,
            email       TEXT NOT NULL DEFAULT '',
            plan        TEXT NOT NULL DEFAULT 'free',
            max_sources INTEGER NOT NULL DEFAULT 1,
            max_listeners INTEGER NOT NULL DEFAULT 25,
            max_bitrate INTEGER NOT NULL DEFAULT 128,
            created_at  TEXT NOT NULL DEFAULT (datetime('now')),
            active      INTEGER NOT NULL DEFAULT 1
        );

        CREATE TABLE IF NOT EXISTS users (
            id           TEXT PRIMARY KEY,
            account_id   TEXT NOT NULL REFERENCES accounts(id),
            username     TEXT NOT NULL UNIQUE,
            password_hash TEXT NOT NULL,
            role         TEXT NOT NULL DEFAULT 'operator',
            created_at   TEXT NOT NULL DEFAULT (datetime('now')),
            active       INTEGER NOT NULL DEFAULT 1
        );

        CREATE TABLE IF NOT EXISTS stations (
            id          TEXT PRIMARY KEY,
            account_id  TEXT NOT NULL REFERENCES accounts(id),
            name        TEXT NOT NULL,
            description TEXT,
            genre       TEXT,
            website     TEXT,
            created_at  TEXT NOT NULL DEFAULT (datetime('now')),
            active      INTEGER NOT NULL DEFAULT 1
        );

        CREATE TABLE IF NOT EXISTS mount_configs (
            id              TEXT PRIMARY KEY,
            station_id      TEXT NOT NULL REFERENCES stations(id),
            account_id      TEXT NOT NULL REFERENCES accounts(id),
            mount_name      TEXT NOT NULL,
            source_password TEXT NOT NULL DEFAULT 'hackme',
            max_listeners   INTEGER,
            bitrate         INTEGER,
            format          TEXT,
            public          INTEGER NOT NULL DEFAULT 1,
            hidden          INTEGER NOT NULL DEFAULT 0,
            fallback_mount  TEXT,
            fallback_when_full INTEGER NOT NULL DEFAULT 0,
            fallback_override   INTEGER NOT NULL DEFAULT 0,
            created_at      TEXT NOT NULL DEFAULT (datetime('now')),
            active          INTEGER NOT NULL DEFAULT 1,
            UNIQUE(account_id, mount_name)
        );

        CREATE TABLE IF NOT EXISTS api_tokens (
            id          TEXT PRIMARY KEY,
            user_id     TEXT NOT NULL REFERENCES users(id),
            token       TEXT NOT NULL UNIQUE,
            expires_at  TEXT,
            created_at  TEXT NOT NULL DEFAULT (datetime('now'))
        );

        CREATE INDEX IF NOT EXISTS idx_users_account ON users(account_id);
        CREATE INDEX IF NOT EXISTS idx_stations_account ON stations(account_id);
        CREATE INDEX IF NOT EXISTS idx_mounts_account ON mount_configs(account_id);
        CREATE INDEX IF NOT EXISTS idx_mounts_station ON mount_configs(station_id);
        CREATE INDEX IF NOT EXISTS idx_mounts_name ON mount_configs(mount_name);
        CREATE INDEX IF NOT EXISTS idx_tokens_user ON api_tokens(user_id);
        ",
    )?;

    info!("Database schema initialized");
    Ok(())
}
