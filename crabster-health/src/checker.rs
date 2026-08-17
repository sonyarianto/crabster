use std::collections::HashMap;
use std::sync::Arc;

use chrono::Utc;

use crate::alerts::{Alert, AlertEngine, AlertSeverity, AlertTrigger};

pub struct HealthStatus {
    pub mount: String,
    pub source_connected: bool,
    pub bitrate: Option<u32>,
    pub bitrate_healthy: bool,
    pub metadata_fresh: bool,
    pub metadata_minutes_since_update: Option<i64>,
    pub listener_count: u32,
    pub listener_trend: String,
    pub buffer_backlog: u64,
    pub overall: HealthLevel,
}

pub enum HealthLevel {
    Green,
    Yellow,
    Red,
}

pub struct HealthChecker {
    alerts: Arc<AlertEngine>,
    prev_listener_counts: Arc<parking_lot::RwLock<HashMap<String, u32>>>,
    prev_metadata_times: Arc<parking_lot::RwLock<HashMap<String, chrono::DateTime<Utc>>>>,
}

impl HealthChecker {
    pub fn new(alerts: Arc<AlertEngine>) -> Self {
        Self {
            alerts,
            prev_listener_counts: Arc::new(parking_lot::RwLock::new(HashMap::new())),
            prev_metadata_times: Arc::new(parking_lot::RwLock::new(HashMap::new())),
        }
    }

    pub fn alerts(&self) -> Vec<Alert> {
        self.alerts.all()
    }

    pub fn alert_engine(&self) -> &AlertEngine {
        &self.alerts
    }

    pub fn check(&self, core: &crabster_core::SharedState) -> Vec<HealthStatus> {
        let sources = core.sources.all_sources();
        let mut statuses = Vec::new();

        for source in &sources {
            let mount = source.info.mount.clone();
            let connected = source.connected.load(std::sync::atomic::Ordering::Relaxed);
            let metadata = source.info.metadata.read();
            let stats = source.info.stats.read();
            let bitrate = metadata.icy_br;
            let buffer_pos = source.buffer.read().current_position();
            let listener_count = stats.current_listeners;

            let bitrate_healthy = match bitrate {
                Some(br) => br >= 16,
                None => connected,
            };

            let now = Utc::now();
            let mut metadata_times = self.prev_metadata_times.write();
            let last_meta_update = metadata_times.entry(mount.clone()).or_insert(now);
            let meta_stale_mins = if metadata.title.is_some() || metadata.icy_name.is_some() {
                let mins = (*last_meta_update - now).num_minutes().abs();
                *last_meta_update = now;
                Some(mins)
            } else {
                None
            };

            let mut prev_counts = self.prev_listener_counts.write();
            let prev = prev_counts.get(&mount).copied().unwrap_or(listener_count);
            let drop_rate = if listener_count < prev && prev > 0 {
                (prev - listener_count) as f64 / prev as f64
            } else {
                0.0
            };
            prev_counts.insert(mount.clone(), listener_count);

            let backlog = buffer_pos;
            let backlog_healthy = backlog < 1_000_000;

            if !connected {
                self.alerts.raise(
                    AlertSeverity::Critical,
                    AlertTrigger::SourceDisconnected,
                    &mount,
                    format!("Source disconnected from {}", mount),
                );
            } else if !bitrate_healthy {
                self.alerts.raise(
                    AlertSeverity::Warning,
                    AlertTrigger::BitrateDropped {
                        expected: 128,
                        actual: bitrate.unwrap_or(0),
                    },
                    &mount,
                    format!("Bitrate drop on {}: {:?}", mount, bitrate),
                );
            }

            if let Some(mins) = meta_stale_mins {
                if mins > 10 {
                    self.alerts.raise(
                        AlertSeverity::Warning,
                        AlertTrigger::MetadataStale {
                            minutes: mins as u32,
                        },
                        &mount,
                        format!("Metadata not updated on {} in {} minutes", mount, mins),
                    );
                }
            }

            if drop_rate > 0.5 {
                self.alerts.raise(
                    AlertSeverity::Warning,
                    AlertTrigger::ListenerDropSpike { rate: drop_rate },
                    &mount,
                    format!(
                        "Listener drop on {}: {:.0}% drop rate",
                        mount,
                        drop_rate * 100.0
                    ),
                );
            }

            if !backlog_healthy {
                self.alerts.raise(
                    AlertSeverity::Info,
                    AlertTrigger::QueueBacklog { bytes: backlog },
                    &mount,
                    format!("Buffer backlog on {}: {} bytes", mount, backlog),
                );
            }

            let overall = if !connected {
                HealthLevel::Red
            } else if !bitrate_healthy || drop_rate > 0.5 {
                HealthLevel::Yellow
            } else {
                HealthLevel::Green
            };

            let listener_trend = if drop_rate > 0.3 {
                "dropping"
            } else if listener_count > 0 && listener_count >= prev {
                "growing"
            } else {
                "stable"
            };

            statuses.push(HealthStatus {
                mount,
                source_connected: connected,
                bitrate,
                bitrate_healthy,
                metadata_fresh: meta_stale_mins.map(|m| m < 10).unwrap_or(true),
                metadata_minutes_since_update: meta_stale_mins,
                listener_count,
                listener_trend: listener_trend.to_string(),
                buffer_backlog: backlog,
                overall,
            });
        }

        statuses
    }

    pub fn start(self: Arc<Self>, core: crabster_core::SharedState) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(10));
            loop {
                interval.tick().await;
                self.check(&core);
            }
        })
    }
}
