use std::sync::Arc;

use tokio::sync::mpsc;

use crate::config::RelayDefinition;

pub enum RelayEvent {
    StreamData(Vec<u8>),
    Metadata(String),
    Disconnect,
}

pub struct RelayConnection {
    pub local_mount: String,
    pub upstream_url: String,
    pub on_demand: bool,
    pub sender: mpsc::UnboundedSender<RelayEvent>,
    pub connected: bool,
}

pub struct RelayManager {
    relays: Vec<Arc<RelayConnection>>,
}

impl RelayManager {
    pub fn new() -> Self {
        Self {
            relays: Vec::new(),
        }
    }

    pub fn add_relay(&mut self, def: &RelayDefinition) {
        let (_tx, _rx) = mpsc::unbounded_channel();
        let uri = def.uri.clone().unwrap_or_else(|| {
            format!(
                "http://{}:{}{}",
                def.server.as_deref().unwrap_or("localhost"),
                def.port.unwrap_or(8000),
                def.mount.as_deref().unwrap_or("/stream")
            )
        });
        self.relays.push(Arc::new(RelayConnection {
            local_mount: def.local_mount.clone(),
            upstream_url: uri,
            on_demand: def.on_demand,
            sender: _tx,
            connected: false,
        }));
    }

    pub fn relays(&self) -> &[Arc<RelayConnection>] {
        &self.relays
    }

    pub fn get_by_mount(&self, mount: &str) -> Option<Arc<RelayConnection>> {
        self.relays.iter().find(|r| r.local_mount == mount).cloned()
    }
}
