use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize)]
pub struct ApiStatus {
    pub version: String,
    pub uptime_seconds: u64,
    pub sources_active: usize,
    pub listeners_total: u64,
    pub bytes_sent: u64,
    pub bytes_received: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MountResponse {
    pub mount: String,
    pub source_connected: bool,
    pub format: String,
    pub bitrate: Option<u32>,
    pub sample_rate: Option<u32>,
    pub channels: Option<u8>,
    pub current_listeners: u32,
    pub peak_listeners: u32,
    pub max_listeners: Option<i64>,
    pub public: bool,
    pub hidden: bool,
    pub connected_at: Option<String>,
    pub metadata: StreamMetadataResponse,
    pub audio_info: std::collections::HashMap<String, String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StreamMetadataResponse {
    pub title: Option<String>,
    pub artist: Option<String>,
    pub song: Option<String>,
    pub description: Option<String>,
    pub genre: Option<String>,
    pub url: Option<String>,
    pub icy_name: Option<String>,
    pub icy_genre: Option<String>,
    pub icy_url: Option<String>,
    pub icy_br: Option<u32>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ListenerResponse {
    pub id: Uuid,
    pub ip: String,
    pub user_agent: String,
    pub connected_seconds: u64,
    pub bytes_received: u64,
    pub country: Option<String>,
    pub referer: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ListenerSummary {
    pub mount: String,
    pub listeners: Vec<ListenerResponse>,
    pub total: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SourceResponse {
    pub mount: String,
    pub connected: bool,
    pub ip: String,
    pub user_agent: String,
    pub format: String,
    pub connected_at: String,
    pub bytes_received: u64,
    pub bitrate: Option<u32>,
    pub audio_info: std::collections::HashMap<String, String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StatsResponse {
    pub server_started: String,
    pub sources_active: usize,
    pub listeners_total: u64,
    pub peak_listeners: u64,
    pub bytes_sent: u64,
    pub bytes_received: u64,
    pub total_connections: u64,
    pub total_source_connections: u64,
    pub mounts: Vec<MountResponse>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ErrorResponse {
    pub error: String,
    pub code: u16,
}

#[derive(Debug, Deserialize)]
pub struct MountQuery {
    pub mount: Option<String>,
}
