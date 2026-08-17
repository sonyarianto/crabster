pub mod admin;
pub mod auth;
pub mod config;
pub mod format;
pub mod fserve;
pub mod listener;
pub mod relay;
pub mod source;
pub mod stats;
pub mod xslt;
pub mod yp;

use std::sync::Arc;
use tokio::sync::RwLock;

pub type SharedState = Arc<AppState>;

pub struct AppState {
    pub config: RwLock<config::Config>,
    pub sources: source::SourceManager,
    pub listeners: listener::ListenerManager,
    pub stats: stats::StatsCollector,
    pub format_registry: format::FormatRegistry,
}
