pub mod collector;
pub mod metrics;

use std::sync::Arc;

use chrono::Utc;
use dashmap::DashMap;
use parking_lot::RwLock;

pub use collector::*;
pub use metrics::*;

pub struct AnalyticsCollector {
    per_mount: DashMap<String, Arc<RwLock<MountAnalytics>>>,
    started_at: std::time::Instant,
}

impl AnalyticsCollector {
    pub fn new() -> Self {
        Self {
            per_mount: DashMap::new(),
            started_at: std::time::Instant::now(),
        }
    }

    pub fn ensure_mount(&self, mount: &str) -> Arc<RwLock<MountAnalytics>> {
        self.per_mount
            .entry(mount.to_string())
            .or_insert_with(|| Arc::new(RwLock::new(MountAnalytics::new(mount))))
            .value()
            .clone()
    }

    pub fn get(&self, mount: &str) -> Option<Arc<RwLock<MountAnalytics>>> {
        self.per_mount.get(mount).map(|r| Arc::clone(&r))
    }

    pub fn all_mounts(&self) -> Vec<Arc<RwLock<MountAnalytics>>> {
        self.per_mount
            .iter()
            .map(|r| Arc::clone(&r.value()))
            .collect()
    }

    pub fn remove_mount(&self, mount: &str) {
        self.per_mount.remove(mount);
    }

    pub fn record_concurrent(&self, mount: &str, count: u32) {
        let analytics = self.ensure_mount(mount);
        let mut a = analytics.write();
        a.concurrent.record(count);
        if count as u64 > a.peak_all_time {
            a.peak_all_time = count as u64;
            a.peak_all_time_at = Some(Utc::now());
        }
    }

    pub fn record_listener_session(
        &self,
        mount: &str,
        duration_secs: u64,
        user_agent: Option<String>,
        _ip: Option<String>,
        referer: Option<String>,
    ) {
        let analytics = self.ensure_mount(mount);
        let mut a = analytics.write();
        a.total_listener_seconds += duration_secs;
        a.total_sessions += 1;

        if let Some(ua) = user_agent {
            let device = classify_user_agent(&ua);
            *a.devices.entry(device).or_insert(0) += 1;
        }
        if let Some(ref r) = referer {
            if !r.is_empty() {
                *a.referrers.entry(r.clone()).or_insert(0) += 1;
            }
        }
    }

    pub fn record_connection(&self, mount: &str) {
        let analytics = self.ensure_mount(mount);
        let mut a = analytics.write();
        a.total_connections += 1;
    }

    pub fn uptime_seconds(&self) -> u64 {
        self.started_at.elapsed().as_secs()
    }
}

fn classify_user_agent(ua: &str) -> String {
    let ua = ua.to_lowercase();
    if ua.contains("iphone") || ua.contains("ipad") || ua.contains("android") && !ua.contains("tv")
    {
        "mobile".into()
    } else if ua.contains("smarttv")
        || ua.contains("tv")
        || ua.contains("roku")
        || ua.contains("firetv")
    {
        "tv".into()
    } else if ua.contains("bot") || ua.contains("crawler") || ua.contains("spider") {
        "bot".into()
    } else if ua.contains("vlc")
        || ua.contains("mpv")
        || ua.contains("ffmpeg")
        || ua.contains("mplayer")
    {
        "media-player".into()
    } else if ua.contains("icecast") || ua.contains("liquidsoap") || ua.contains("butt") {
        "encoder".into()
    } else {
        "desktop".into()
    }
}
