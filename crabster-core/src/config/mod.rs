use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub hostname: String,
    pub listen_sockets: Vec<ListenSocket>,
    pub authentication: Authentication,
    pub limits: Limits,
    pub logging: Logging,
    pub paths: Paths,
    pub mounts: Vec<MountConfig>,
    pub shoutcast_mount: Option<String>,
    pub http_headers: Vec<HttpHeader>,
    pub relay: Option<RelayConfig>,
    pub security: Option<SecurityConfig>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            hostname: "localhost".into(),
            listen_sockets: vec![ListenSocket {
                port: 8000,
                bind_address: None,
                tls: TlsMode::Auto,
                shoutcast_compat: false,
                shoutcast_mount: None,
            }],
            authentication: Authentication {
                source_password: "hackme".into(),
                admin_user: "admin".into(),
                admin_password: "hackme".into(),
            },
            limits: Limits {
                clients: 100,
                sources: 2,
                queue_size: 524288,
                client_timeout: 30,
                header_timeout: 15,
                source_timeout: 10,
                burst_size: 65535,
                body_timeout: 30,
                body_size_limit: 1048576,
            },
            logging: Logging {
                loglevel: LogLevel::Information,
                accesslog: "access.log".into(),
                errorlog: "error.log".into(),
                playlistlog: None,
                logsize: 10000,
                logarchive: false,
            },
            paths: Paths {
                logdir: "/var/log/crabster".into(),
                webroot: "/usr/share/crabster/web".into(),
                adminroot: "/usr/share/crabster/admin".into(),
                pidfile: None,
                basedir: None,
            },
            mounts: Vec::new(),
            shoutcast_mount: None,
            http_headers: vec![HttpHeader {
                header_type: Some(HeaderType::Cors),
                name: "Access-Control-Allow-Origin".into(),
                value: None,
            }],
            relay: None,
            security: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListenSocket {
    pub port: u16,
    pub bind_address: Option<String>,
    pub tls: TlsMode,
    pub shoutcast_compat: bool,
    pub shoutcast_mount: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TlsMode {
    Auto,
    AutoNoPlain,
    Rfc2818,
    Disabled,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Authentication {
    pub source_password: String,
    pub admin_user: String,
    pub admin_password: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Limits {
    pub clients: u32,
    pub sources: u32,
    pub queue_size: u32,
    pub client_timeout: u32,
    pub header_timeout: u32,
    pub source_timeout: u32,
    pub burst_size: u32,
    pub body_timeout: u32,
    pub body_size_limit: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Logging {
    pub loglevel: LogLevel,
    pub accesslog: String,
    pub errorlog: String,
    pub playlistlog: Option<String>,
    pub logsize: u32,
    pub logarchive: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LogLevel {
    Debug,
    Information,
    Warning,
    Error,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Paths {
    pub logdir: String,
    pub webroot: String,
    pub adminroot: String,
    pub pidfile: Option<String>,
    pub basedir: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MountConfig {
    pub mount_name: String,
    pub max_listeners: Option<i64>,
    pub burst_size: Option<u32>,
    pub fallback_mount: Option<String>,
    pub fallback_override: Option<bool>,
    pub fallback_when_full: Option<bool>,
    pub intro: Option<String>,
    pub hidden: Option<bool>,
    pub public: Option<bool>,
    pub dump_file: Option<String>,
    pub authentication: Option<MountAuth>,
    pub http_headers: Option<Vec<HttpHeader>>,
    pub relay: Option<MountRelay>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MountAuth {
    pub roles: Vec<AuthRole>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthRole {
    pub role_type: String,
    pub options: HashMap<String, String>,
    pub methods: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MountRelay {
    pub upstream_urls: Vec<String>,
    pub on_demand: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpHeader {
    pub header_type: Option<HeaderType>,
    pub name: String,
    pub value: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum HeaderType {
    Cors,
    Custom,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RelayConfig {
    pub master_server: Option<String>,
    pub master_server_port: Option<u16>,
    pub master_update_interval: Option<u32>,
    pub master_password: Option<String>,
    pub relays_on_demand: Option<bool>,
    pub relays: Vec<RelayDefinition>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RelayDefinition {
    pub local_mount: String,
    pub on_demand: bool,
    pub upstream_type: String,
    pub uri: Option<String>,
    pub server: Option<String>,
    pub port: Option<u16>,
    pub mount: Option<String>,
    pub relay_shoutcast_metadata: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecurityConfig {
    pub chroot: Option<bool>,
    pub changeowner: Option<ChangeOwner>,
    pub tls_certificate: Option<String>,
    pub tls_key: Option<String>,
    pub ban_file: Option<String>,
    pub allow_file: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChangeOwner {
    pub user: Option<String>,
    pub group: Option<String>,
}
