use std::sync::Arc;

use crate::AnalyticsCollector;

pub fn start_collector(
    analytics: Arc<AnalyticsCollector>,
    core: crabster_core::SharedState,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(1));
        loop {
            interval.tick().await;
            let mounts = core.sources.all_sources();
            for source in &mounts {
                let mount = &source.info.mount;
                let listener_count = source.info.stats.read().current_listeners;
                analytics.record_concurrent(mount, listener_count);
            }
        }
    })
}
