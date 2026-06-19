use std::collections::VecDeque;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::Serialize;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum AlertSeverity {
    Info,
    Warning,
    Critical,
}

#[derive(Debug, Clone, Serialize)]
pub enum AlertTrigger {
    SourceDisconnected,
    BitrateDropped { expected: u32, actual: u32 },
    MetadataStale { minutes: u32 },
    ListenerDropSpike { rate: f64 },
    QueueBacklog { bytes: u64 },
}

#[derive(Debug, Clone, Serialize)]
pub struct Alert {
    pub id: Uuid,
    pub severity: AlertSeverity,
    pub trigger: AlertTrigger,
    pub mount: String,
    pub message: String,
    pub timestamp: DateTime<Utc>,
    pub acknowledged: bool,
    pub resolved: bool,
}

pub struct AlertEngine {
    alerts: Arc<RwLock<VecDeque<Alert>>>,
    max_alerts: usize,
}

impl AlertEngine {
    pub fn new(max_alerts: usize) -> Self {
        Self {
            alerts: Arc::new(RwLock::new(VecDeque::with_capacity(max_alerts))),
            max_alerts,
        }
    }

    pub fn raise(&self, severity: AlertSeverity, trigger: AlertTrigger, mount: &str, message: String) {
        let mut alerts = self.alerts.write();
        if alerts.len() >= self.max_alerts {
            alerts.pop_front();
        }
        alerts.push_back(Alert {
            id: Uuid::new_v4(),
            severity,
            trigger,
            mount: mount.to_string(),
            message,
            timestamp: Utc::now(),
            acknowledged: false,
            resolved: false,
        });
    }

    pub fn acknowledge(&self, id: &Uuid) {
        if let Some(alert) = self.alerts.write().iter_mut().find(|a| a.id == *id) {
            alert.acknowledged = true;
        }
    }

    pub fn resolve(&self, id: &Uuid) {
        if let Some(alert) = self.alerts.write().iter_mut().find(|a| a.id == *id) {
            alert.resolved = true;
        }
    }

    pub fn active(&self) -> Vec<Alert> {
        self.alerts
            .read()
            .iter()
            .filter(|a| !a.resolved)
            .cloned()
            .collect()
    }

    pub fn all(&self) -> Vec<Alert> {
        self.alerts.read().iter().cloned().collect()
    }

    pub fn arc() -> (Arc<RwLock<VecDeque<Alert>>>, Arc<Self>) {
        let engine = Arc::new(Self::new(1000));
        (Arc::clone(&engine.alerts), engine)
    }
}
