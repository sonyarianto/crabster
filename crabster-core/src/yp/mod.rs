use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use tracing::{info, warn};

use crate::source::{Source, SourceInfo, StreamMetadata};
use crate::SharedState;

/// YP (Yellow Pages) directory configuration, mirroring Icecast's
/// `<directory>` section. When configured, public mounts are listed in the
/// directory and kept alive with periodic touch requests.
#[derive(Debug, Clone)]
pub struct YpConfig {
    /// Directory submission URL, e.g. `http://dir.xiph.org/cgi-bin/yp-cgi`.
    pub url: String,
    /// Hostname advertised in the listen URL (must be publicly reachable).
    pub hostname: String,
    /// Stream port used to build the listen URL.
    pub stream_port: u16,
    /// HTTP timeout for directory requests.
    pub timeout: Duration,
    /// How often to poll the source list for changes.
    pub poll_interval: Duration,
    /// Default interval between touch updates (the directory may override
    /// this via the `TouchFreq` response header; clamped to at least 30s).
    pub touch_interval: Duration,
}

impl Default for YpConfig {
    fn default() -> Self {
        Self {
            url: "http://dir.xiph.org/cgi-bin/yp-cgi".into(),
            hostname: "localhost".into(),
            stream_port: 8000,
            timeout: Duration::from_secs(15),
            poll_interval: Duration::from_secs(5),
            touch_interval: Duration::from_secs(300),
        }
    }
}

/// Per-mount YP state.
struct YpEntry {
    /// Session id returned by the directory after a successful add; `None`
    /// while waiting to retry the add.
    sid: Option<String>,
    /// When the next add/touch request for this mount is due.
    next_update: Instant,
    /// Touch interval for this mount (from `TouchFreq` or the config default).
    touch_interval: Duration,
}

/// Parsed directory response headers (Icecast YP protocol).
struct YpResponse {
    ok: bool,
    message: Option<String>,
    sid: Option<String>,
    touch_freq: Option<Duration>,
}

/// Publishes public mounts to a YP directory. A background task polls the
/// source list: new public mounts get an `add`, listed mounts get a periodic
/// `touch` with the current listener count and song, and removed mounts get a
/// `remove`.
pub struct YpManager {
    config: YpConfig,
    client: reqwest::Client,
    state: SharedState,
    entries: Mutex<HashMap<String, YpEntry>>,
}

impl YpManager {
    pub fn new(config: YpConfig, state: SharedState) -> Self {
        let client = reqwest::Client::builder()
            .timeout(config.timeout)
            .user_agent(format!("Crabster/{}", env!("CARGO_PKG_VERSION")))
            .build()
            .expect("failed to build YP HTTP client");
        Self {
            config,
            client,
            state,
            entries: Mutex::new(HashMap::new()),
        }
    }

    /// Starts the background YP worker.
    pub fn start(self: Arc<Self>) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move { self.run().await })
    }

    async fn run(self: Arc<Self>) {
        loop {
            self.poll().await;
            tokio::time::sleep(self.config.poll_interval).await;
        }
    }

    fn public_sources(&self) -> Vec<(String, Arc<Source>)> {
        self.state
            .sources
            .all_sources()
            .into_iter()
            .filter(|s| s.info.public)
            .map(|s| (s.info.mount.clone(), s))
            .collect()
    }

    async fn poll(&self) {
        let sources = self.public_sources();
        let active: HashSet<String> = sources.iter().map(|(m, _)| m.clone()).collect();

        // Remove entries for mounts that are gone or no longer public.
        let stale: Vec<(String, Option<String>)> = {
            let mut entries = self.entries.lock();
            let stale: Vec<String> = entries
                .keys()
                .filter(|m| !active.contains(*m))
                .cloned()
                .collect();
            stale
                .into_iter()
                .map(|mount| {
                    let entry = entries.remove(&mount).expect("entry exists");
                    (mount, entry.sid)
                })
                .collect()
        };
        for (mount, sid) in stale {
            self.remove(&mount, sid.as_deref()).await;
        }

        // Add or touch active mounts.
        for (mount, source) in &sources {
            enum Due {
                Add,
                Touch,
            }
            let due = {
                let entries = self.entries.lock();
                match entries.get(mount) {
                    None => Some(Due::Add),
                    Some(entry) if entry.next_update <= Instant::now() => {
                        if entry.sid.is_none() {
                            Some(Due::Add)
                        } else {
                            Some(Due::Touch)
                        }
                    }
                    Some(_) => None,
                }
            };
            match due {
                Some(Due::Add) => self.add(mount, &source.info).await,
                Some(Due::Touch) => self.touch(mount, &source.info).await,
                None => {}
            }
        }
    }

    /// Sends an `action=add` request and records the returned session id.
    async fn add(&self, mount: &str, info: &SourceInfo) {
        let params = {
            // Scoped so the read guard is dropped before the HTTP await.
            let meta = info.metadata.read();
            self.build_add_params(mount, info, &meta)
        };

        match self.post(&params).await {
            Some(resp) if resp.ok => {
                info!("YP listed {} at {}", mount, self.config.url);
                let interval = resp
                    .touch_freq
                    .unwrap_or(self.config.touch_interval)
                    .max(Duration::from_secs(30));
                let mut entries = self.entries.lock();
                entries.insert(
                    mount.to_string(),
                    YpEntry {
                        sid: resp.sid,
                        // Force the first touch soon after the add, like Icecast.
                        next_update: Instant::now() + Duration::from_secs(5),
                        touch_interval: interval,
                    },
                );
            }
            Some(resp) => {
                warn!(
                    "YP add failed for {}: {}",
                    mount,
                    resp.message.as_deref().unwrap_or("unknown error")
                );
                self.schedule_retry(mount);
            }
            None => {
                warn!(
                    "YP request to {} failed for mount {}",
                    self.config.url, mount
                );
                self.schedule_retry(mount);
            }
        }
    }

    /// Sends an `action=touch` request with current listener/song data.
    async fn touch(&self, mount: &str, info: &SourceInfo) {
        let (sid, interval) = {
            let entries = self.entries.lock();
            match entries.get(mount) {
                Some(entry) => (entry.sid.clone(), entry.touch_interval),
                None => return,
            }
        };
        let Some(sid) = sid else {
            return;
        };

        let params = {
            // Scoped so the read guard is dropped before the HTTP await.
            let meta = info.metadata.read();
            self.build_touch_params(&sid, info, &meta)
        };

        match self.post(&params).await {
            Some(resp) if resp.ok => {
                let interval = resp
                    .touch_freq
                    .unwrap_or(interval)
                    .max(Duration::from_secs(30));
                let mut entries = self.entries.lock();
                if let Some(entry) = entries.get_mut(mount) {
                    entry.next_update = Instant::now() + interval;
                    entry.touch_interval = interval;
                }
            }
            _ => {
                // Touch rejected: drop the session and fall back to a fresh add.
                warn!("YP touch failed for {}, will re-add", mount);
                let mut entries = self.entries.lock();
                if let Some(entry) = entries.get_mut(mount) {
                    entry.sid = None;
                    entry.next_update = Instant::now() + Duration::from_secs(600);
                }
            }
        }
    }

    /// Sends an `action=remove` request (best effort) for a gone mount.
    async fn remove(&self, mount: &str, sid: Option<&str>) {
        if let Some(sid) = sid {
            let params = HashMap::from([
                ("action".to_string(), "remove".to_string()),
                ("sid".to_string(), sid.to_string()),
            ]);
            let _ = self.post(&params).await;
        }
        info!("YP entry removed for {}", mount);
    }

    fn schedule_retry(&self, mount: &str) {
        let mut entries = self.entries.lock();
        entries.insert(
            mount.to_string(),
            YpEntry {
                sid: None,
                next_update: Instant::now() + Duration::from_secs(600),
                touch_interval: self.config.touch_interval,
            },
        );
    }

    async fn post(&self, params: &HashMap<String, String>) -> Option<YpResponse> {
        let resp = self
            .client
            .post(&self.config.url)
            .form(params)
            .send()
            .await
            .ok()?;
        let headers = resp.headers();
        let header_str = |name: &str| {
            headers
                .get(name)
                .and_then(|v| v.to_str().ok())
                .map(|v| v.trim().to_string())
        };
        Some(YpResponse {
            ok: header_str("YPResponse")
                .map(|v| v.starts_with('1'))
                .unwrap_or(false),
            message: header_str("YPMessage"),
            sid: header_str("SID"),
            touch_freq: header_str("TouchFreq")
                .and_then(|v| v.parse::<u64>().ok())
                .map(Duration::from_secs),
        })
    }

    fn build_add_params(
        &self,
        mount: &str,
        info: &SourceInfo,
        meta: &StreamMetadata,
    ) -> HashMap<String, String> {
        HashMap::from([
            ("action".to_string(), "add".to_string()),
            (
                "sn".to_string(),
                meta.icy_name.clone().unwrap_or_else(|| mount.to_string()),
            ),
            (
                "genre".to_string(),
                meta.icy_genre.clone().unwrap_or_default(),
            ),
            ("type".to_string(), info.format.mime_type().to_string()),
            (
                "b".to_string(),
                meta.icy_br.map(|b| b.to_string()).unwrap_or_default(),
            ),
            (
                "desc".to_string(),
                meta.description.clone().unwrap_or_default(),
            ),
            ("url".to_string(), meta.icy_url.clone().unwrap_or_default()),
            (
                "listenurl".to_string(),
                build_listen_url(&self.config.hostname, self.config.stream_port, mount),
            ),
            ("stype".to_string(), String::new()),
        ])
    }

    fn build_touch_params(
        &self,
        sid: &str,
        info: &SourceInfo,
        meta: &StreamMetadata,
    ) -> HashMap<String, String> {
        let song = match (&meta.artist, &meta.title) {
            (Some(artist), Some(title)) => format!("{} - {}", artist, title),
            (_, Some(title)) => title.clone(),
            _ => String::new(),
        };
        let listeners = info.stats.read().current_listeners.to_string();
        HashMap::from([
            ("action".to_string(), "touch".to_string()),
            ("sid".to_string(), sid.to_string()),
            ("st".to_string(), song),
            ("listeners".to_string(), listeners),
            ("max_listeners".to_string(), String::new()),
            ("stype".to_string(), String::new()),
        ])
    }
}

/// The listener URL advertised to the directory for a mount.
fn build_listen_url(hostname: &str, port: u16, mount: &str) -> String {
    format!("http://{}:{}{}", hostname, port, mount)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn listen_url_uses_host_port_and_mount() {
        assert_eq!(
            build_listen_url("radio.example.org", 8000, "/live"),
            "http://radio.example.org:8000/live"
        );
    }
}
