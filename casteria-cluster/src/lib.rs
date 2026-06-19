pub mod origin;
pub mod edge;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterConfig {
    pub enabled: bool,
    pub mode: ClusterMode,
    pub bind_address: Option<String>,
    pub origin_host: Option<String>,
    pub origin_port: u16,
    pub relays: Vec<RelayDef>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ClusterMode {
    Standalone,
    Origin,
    Edge,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RelayDef {
    pub local_mount: String,
    pub upstream_uri: Option<String>,
    pub on_demand: bool,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            mode: ClusterMode::Standalone,
            bind_address: None,
            origin_host: None,
            origin_port: 8002,
            relays: Vec::new(),
        }
    }
}
