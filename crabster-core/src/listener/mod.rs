use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use dashmap::DashMap;
use parking_lot::RwLock;
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::source::{Source, StreamMetadata};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ListenerProtocol {
    Http,
    Icy,
    Hls,
}

#[derive(Debug, Clone)]
pub struct ListenerInfo {
    pub id: Uuid,
    pub mount: String,
    pub ip: String,
    pub user_agent: String,
    pub protocol: ListenerProtocol,
    pub connected_at: Instant,
    pub bytes_sent: u64,
    pub referer: Option<String>,
    pub country: Option<String>,
}

pub struct Listener {
    pub info: Arc<RwLock<ListenerInfo>>,
    pub sender: mpsc::UnboundedSender<ListenerEvent>,
    pub disconnected: AtomicBool,
}

pub enum ListenerEvent {
    Data(Bytes),
    Metadata(Arc<RwLock<StreamMetadata>>),
    Disconnect,
}

pub struct ListenerManager {
    listeners: DashMap<String, Vec<Arc<Listener>>>,
    total_listeners: AtomicU64,
}

impl ListenerManager {
    pub fn new() -> Self {
        Self {
            listeners: DashMap::new(),
            total_listeners: AtomicU64::new(0),
        }
    }

    pub fn add_listener(&self, mount: String, listener: Arc<Listener>) {
        self.listeners
            .entry(mount)
            .or_insert_with(Vec::new)
            .push(listener);
        self.total_listeners.fetch_add(1, Ordering::Relaxed);
    }

    pub fn remove_listener(&self, mount: &str, id: Uuid) {
        if let Some(mut entry) = self.listeners.get_mut(mount) {
            entry.retain(|l| l.info.read().id != id);
            if entry.is_empty() {
                drop(entry);
                self.listeners.remove(mount);
            }
        }
        self.total_listeners.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn listener_count(&self, mount: &str) -> usize {
        self.listeners.get(mount).map(|e| e.len()).unwrap_or(0)
    }

    pub fn total_count(&self) -> u64 {
        self.total_listeners.load(Ordering::Relaxed)
    }

    pub fn get_listeners(&self, mount: &str) -> Vec<Arc<Listener>> {
        self.listeners
            .get(mount)
            .map(|e| e.clone())
            .unwrap_or_default()
    }

    pub fn mount_count(&self) -> usize {
        self.listeners.len()
    }

    pub fn all_listeners(&self) -> Vec<Arc<Listener>> {
        self.listeners
            .iter()
            .flat_map(|entry| entry.value().clone())
            .collect()
    }

    pub fn has_mount(&self, mount: &str) -> bool {
        self.listeners.contains_key(mount)
    }

    pub fn kick_listener(&self, mount: &str, id: Uuid) -> bool {
        if let Some(mut entry) = self.listeners.get_mut(mount) {
            if let Some(pos) = entry.iter().position(|l| l.info.read().id == id) {
                let listener = entry.remove(pos);
                listener.disconnected.store(true, Ordering::Relaxed);
                let _ = listener.sender.send(ListenerEvent::Disconnect);
                self.total_listeners.fetch_sub(1, Ordering::Relaxed);
                return true;
            }
        }
        false
    }

    pub fn broadcast_data(&self, mount: &str, data: Bytes, _source: &Source) {
        if let Some(entry) = self.listeners.get(mount) {
            for listener in entry.value().iter() {
                if listener.disconnected.load(Ordering::Relaxed) {
                    continue;
                }
                if let Err(_) = listener.sender.send(ListenerEvent::Data(data.clone())) {
                    listener.disconnected.store(true, Ordering::Relaxed);
                }
            }
        }
    }

    pub fn broadcast_metadata(&self, mount: &str, metadata: Arc<RwLock<StreamMetadata>>) {
        if let Some(entry) = self.listeners.get(mount) {
            for listener in entry.value().iter() {
                if listener.disconnected.load(Ordering::Relaxed) {
                    continue;
                }
                if let Err(_) = listener
                    .sender
                    .send(ListenerEvent::Metadata(Arc::clone(&metadata)))
                {
                    listener.disconnected.store(true, Ordering::Relaxed);
                }
            }
        }
    }
}
