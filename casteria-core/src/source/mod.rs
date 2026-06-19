use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use dashmap::DashMap;
use parking_lot::RwLock as PLRwLock;
use uuid::Uuid;

use crate::format::FormatType;
use crate::stats::MountStats;

#[derive(Debug, Clone)]
pub struct SourceInfo {
    pub id: Uuid,
    pub mount: String,
    pub connected_at: Instant,
    pub client_ip: String,
    pub user_agent: String,
    pub format: FormatType,
    pub audio_info: HashMap<String, String>,
    pub bitrate: Option<u32>,
    pub quality: Option<f32>,
    pub channels: Option<u8>,
    pub sample_rate: Option<u32>,
    pub max_listeners: Option<usize>,
    pub public: bool,
    pub hidden: bool,
    pub fallback_mount: Option<String>,
    pub fallback_override: bool,
    pub fallback_when_full: bool,
    pub burst_size: usize,
    pub metadata: Arc<PLRwLock<StreamMetadata>>,
    pub stats: Arc<PLRwLock<MountStats>>,
}

#[derive(Debug, Clone)]
pub struct StreamMetadata {
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
    pub audio_info: HashMap<String, String>,
}

impl Default for StreamMetadata {
    fn default() -> Self {
        Self {
            title: None,
            artist: None,
            song: None,
            description: None,
            genre: None,
            url: None,
            icy_name: None,
            icy_genre: None,
            icy_url: None,
            icy_br: None,
            audio_info: HashMap::new(),
        }
    }
}

pub struct RingBuffer {
    buffer: Vec<u8>,
    capacity: usize,
    write_pos: AtomicU64,
}

impl RingBuffer {
    pub fn new(capacity: usize) -> Self {
        Self {
            buffer: vec![0u8; capacity],
            capacity,
            write_pos: AtomicU64::new(0),
        }
    }

    pub fn write(&mut self, data: &[u8]) {
        let len = data.len();
        if len >= self.capacity {
            let offset = len - self.capacity;
            let tail = &data[offset..];
            self.buffer.copy_from_slice(tail);
            self.write_pos.fetch_add(len as u64, Ordering::AcqRel);
            return;
        }

        let pos = self.write_pos.load(Ordering::Acquire) as usize % self.capacity;
        let end = pos + len;
        if end <= self.capacity {
            self.buffer[pos..end].copy_from_slice(data);
        } else {
            let first = self.capacity - pos;
            self.buffer[pos..].copy_from_slice(&data[..first]);
            self.buffer[..end % self.capacity].copy_from_slice(&data[first..]);
        }
        self.write_pos.fetch_add(len as u64, Ordering::AcqRel);
    }

    pub fn read(&self, offset: u64) -> Bytes {
        let write_pos = self.write_pos.load(Ordering::Acquire);
        let readable = (write_pos - offset) as usize;
        if readable == 0 {
            return Bytes::new();
        }
        let len = readable.min(self.capacity);
        let start = (offset as usize) % self.capacity;
        if start + len <= self.capacity {
            Bytes::copy_from_slice(&self.buffer[start..start + len])
        } else {
            let first = self.capacity - start;
            let mut buf = Vec::with_capacity(len);
            buf.extend_from_slice(&self.buffer[start..]);
            buf.extend_from_slice(&self.buffer[..len - first]);
            Bytes::from(buf)
        }
    }

    pub fn current_position(&self) -> u64 {
        self.write_pos.load(Ordering::Acquire)
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

pub struct Source {
    pub info: Arc<SourceInfo>,
    pub buffer: Arc<PLRwLock<RingBuffer>>,
    pub connected: AtomicBool,
    pub running: AtomicBool,
}

pub struct SourceManager {
    sources: DashMap<String, Arc<Source>>,
}

impl SourceManager {
    pub fn new() -> Self {
        Self {
            sources: DashMap::new(),
        }
    }

    pub fn register(&self, mount: String, source: Arc<Source>) -> bool {
        if self.sources.contains_key(&mount) {
            return false;
        }
        self.sources.insert(mount, source);
        true
    }

    pub fn unregister(&self, mount: &str) -> Option<Arc<Source>> {
        self.sources.remove(mount).map(|(_, v)| v)
    }

    pub fn get(&self, mount: &str) -> Option<Arc<Source>> {
        self.sources.get(mount).map(|r| Arc::clone(&r))
    }

    pub fn get_source_info(&self, mount: &str) -> Option<Arc<SourceInfo>> {
        self.sources.get(mount).map(|r| Arc::clone(&r.info))
    }

    pub fn mount_exists(&self, mount: &str) -> bool {
        self.sources.contains_key(mount)
    }

    pub fn all_mounts(&self) -> Vec<String> {
        self.sources.iter().map(|r| r.key().clone()).collect()
    }

    pub fn all_sources(&self) -> Vec<Arc<Source>> {
        self.sources.iter().map(|r| Arc::clone(&r.value())).collect()
    }

    pub fn count(&self) -> usize {
        self.sources.len()
    }
}
