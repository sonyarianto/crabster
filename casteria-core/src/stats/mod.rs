use std::sync::atomic::{AtomicU32, AtomicU64};
use std::sync::Arc;
use std::time::Instant;

use chrono::{DateTime, Utc};
use dashmap::DashMap;
use parking_lot::RwLock;

use crate::source::StreamMetadata;

#[derive(Debug, Clone)]
pub struct MountStats {
    pub mount: String,
    pub source_connected: bool,
    pub source_connected_at: Option<Instant>,
    pub source_ip: Option<String>,
    pub source_user_agent: Option<String>,
    pub total_bytes_received: u64,
    pub total_bytes_sent: u64,
    pub total_listener_connections: u64,
    pub current_listeners: u32,
    pub peak_listeners: u32,
    pub peak_listeners_at: Option<DateTime<Utc>>,
    pub max_listeners: Option<i64>,
    pub started_at: Option<DateTime<Utc>>,
    pub last_listener_connect: Option<DateTime<Utc>>,
    pub last_listener_disconnect: Option<DateTime<Utc>>,
    pub bitrate: Option<u32>,
    pub sample_rate: Option<u32>,
    pub channels: Option<u8>,
    pub format: Option<String>,
    pub server_name: Option<String>,
    pub server_url: Option<String>,
    pub genre: Option<String>,
    pub description: Option<String>,
    pub listener_seconds: u64,
    pub metadata_updates: u64,
}

impl MountStats {
    pub fn new(mount: &str) -> Self {
        Self {
            mount: mount.to_string(),
            source_connected: false,
            source_connected_at: None,
            source_ip: None,
            source_user_agent: None,
            total_bytes_received: 0,
            total_bytes_sent: 0,
            total_listener_connections: 0,
            current_listeners: 0,
            peak_listeners: 0,
            peak_listeners_at: None,
            max_listeners: None,
            started_at: Some(Utc::now()),
            last_listener_connect: None,
            last_listener_disconnect: None,
            bitrate: None,
            sample_rate: None,
            channels: None,
            format: None,
            server_name: None,
            server_url: None,
            genre: None,
            description: None,
            listener_seconds: 0,
            metadata_updates: 0,
        }
    }
}

pub struct StatsCollector {
    global: GlobalStats,
    per_mount: DashMap<String, Arc<RwLock<MountStats>>>,
    mount_metadata: DashMap<String, Arc<RwLock<StreamMetadata>>>,
}

#[derive(Debug)]
pub struct GlobalStats {
    pub started_at: Instant,
    pub total_connections: AtomicU64,
    pub total_source_connections: AtomicU64,
    pub current_sources: AtomicU32,
    pub peak_sources: AtomicU32,
    pub current_listeners: AtomicU64,
    pub peak_listeners: AtomicU64,
    pub total_bytes_sent: AtomicU64,
    pub total_bytes_received: AtomicU64,
}

impl GlobalStats {
    pub fn new() -> Self {
        Self {
            started_at: Instant::now(),
            total_connections: AtomicU64::new(0),
            total_source_connections: AtomicU64::new(0),
            current_sources: AtomicU32::new(0),
            peak_sources: AtomicU32::new(0),
            current_listeners: AtomicU64::new(0),
            peak_listeners: AtomicU64::new(0),
            total_bytes_sent: AtomicU64::new(0),
            total_bytes_received: AtomicU64::new(0),
        }
    }
}

impl StatsCollector {
    pub fn new() -> Self {
        Self {
            global: GlobalStats::new(),
            per_mount: DashMap::new(),
            mount_metadata: DashMap::new(),
        }
    }

    pub fn global(&self) -> &GlobalStats {
        &self.global
    }

    pub fn get_mount_stats(&self, mount: &str) -> Option<Arc<RwLock<MountStats>>> {
        self.per_mount.get(mount).map(|r| Arc::clone(&r))
    }

    pub fn ensure_mount(&self, mount: &str) -> Arc<RwLock<MountStats>> {
        self.per_mount
            .entry(mount.to_string())
            .or_insert_with(|| Arc::new(RwLock::new(MountStats::new(mount))))
            .value()
            .clone()
    }

    pub fn get_mounts(&self) -> Vec<Arc<RwLock<MountStats>>> {
        self.per_mount
            .iter()
            .map(|r| Arc::clone(&r.value()))
            .collect()
    }

    pub fn remove_mount(&self, mount: &str) {
        self.per_mount.remove(mount);
        self.mount_metadata.remove(mount);
    }

    pub fn get_or_create_metadata(&self, mount: &str) -> Arc<RwLock<StreamMetadata>> {
        self.mount_metadata
            .entry(mount.to_string())
            .or_insert_with(|| Arc::new(RwLock::new(StreamMetadata::default())))
            .value()
            .clone()
    }

    pub fn get_metadata(&self, mount: &str) -> Option<Arc<RwLock<StreamMetadata>>> {
        self.mount_metadata
            .get(mount)
            .map(|r| Arc::clone(&r.value()))
    }

    pub fn update_metadata(&self, mount: &str, metadata: StreamMetadata) {
        if let Some(existing) = self.mount_metadata.get(mount) {
            *existing.write() = metadata;
        } else {
            self.mount_metadata
                .insert(mount.to_string(), Arc::new(RwLock::new(metadata)));
        }
        if let Some(stats) = self.per_mount.get(mount) {
            stats.write().metadata_updates += 1;
        }
    }
}
