use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use parking_lot::RwLock;

use casteria_core::SharedState;

#[derive(Debug, Clone)]
pub struct HlsConfig {
    pub segment_duration: Duration,
    pub window_size: usize,
    pub poll_interval: Duration,
}

impl Default for HlsConfig {
    fn default() -> Self {
        Self {
            segment_duration: Duration::from_secs(10),
            window_size: 5,
            poll_interval: Duration::from_millis(200),
        }
    }
}

#[derive(Debug, Clone)]
pub struct HlsSegment {
    pub sequence: u64,
    pub data: Bytes,
    pub duration: f64,
}

pub struct HlsSession {
    mount: String,
    config: HlsConfig,
    segments: VecDeque<HlsSegment>,
    current_data: Vec<u8>,
    current_start: Instant,
    segment_counter: u64,
    last_read_pos: u64,
    last_data_time: Instant,
}

impl HlsSession {
    pub fn new(mount: String, config: HlsConfig) -> Self {
        Self {
            mount,
            segments: VecDeque::with_capacity(config.window_size + 1),
            current_data: Vec::with_capacity(65536),
            current_start: Instant::now(),
            segment_counter: 0,
            last_read_pos: 0,
            last_data_time: Instant::now(),
            config,
        }
    }

    fn feed(&mut self, data: &[u8]) {
        self.current_data.extend_from_slice(data);
        self.last_data_time = Instant::now();

        if self.current_start.elapsed() >= self.config.segment_duration {
            self.seal_segment();
        }
    }

    fn seal_segment(&mut self) {
        if self.current_data.is_empty() {
            return;
        }

        let elapsed = self.current_start.elapsed().as_secs_f64();
        let segment = HlsSegment {
            sequence: self.segment_counter,
            data: Bytes::copy_from_slice(&self.current_data),
            duration: elapsed.max(1.0),
        };

        self.segments.push_back(segment);
        if self.segments.len() > self.config.window_size {
            self.segments.pop_front();
        }

        self.current_data.clear();
        self.current_start = Instant::now();
        self.segment_counter += 1;
    }

    fn force_seal(&mut self) {
        if !self.current_data.is_empty() {
            let elapsed = self.current_start.elapsed().as_secs_f64();
            let segment = HlsSegment {
                sequence: self.segment_counter,
                data: Bytes::copy_from_slice(&self.current_data),
                duration: elapsed.max(1.0),
            };
            self.segments.push_back(segment);
            if self.segments.len() > self.config.window_size {
                self.segments.pop_front();
            }
            self.current_data.clear();
            self.segment_counter += 1;
        }
    }

    pub fn get_playlist(&self) -> String {
        let target = self.config.segment_duration.as_secs_f64().ceil() as u64;
        let media_seq = self.segments.front().map(|s| s.sequence).unwrap_or(0);

        let mut lines = Vec::new();
        lines.push("#EXTM3U".to_string());
        lines.push("#EXT-X-VERSION:3".to_string());
        lines.push(format!("#EXT-X-TARGETDURATION:{}", target));
        lines.push(format!("#EXT-X-MEDIA-SEQUENCE:{}", media_seq));

        for seg in &self.segments {
            lines.push(format!("#EXTINF:{:.3},", seg.duration));
            lines.push(format!("segment-{}.ts", seg.sequence));
        }

        lines.push(String::new());
        lines.join("\n")
    }

    pub fn get_segment(&self, sequence: u64) -> Option<Bytes> {
        self.segments
            .iter()
            .find(|s| s.sequence == sequence)
            .map(|s| s.data.clone())
    }

    pub fn has_segment(&self, sequence: u64) -> bool {
        self.segments.iter().any(|s| s.sequence == sequence)
    }

    pub fn mount(&self) -> &str {
        &self.mount
    }

    pub fn is_stale(&self, timeout: Duration) -> bool {
        self.last_data_time.elapsed() > timeout
    }

    pub fn should_seal(&self) -> bool {
        !self.current_data.is_empty()
            && self.current_start.elapsed() >= self.config.segment_duration
    }
}

pub struct HlsManager {
    config: HlsConfig,
    sessions: Arc<RwLock<HashMap<String, HlsSession>>>,
}

impl HlsManager {
    pub fn new(config: HlsConfig) -> Self {
        Self {
            config,
            sessions: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn has_session(&self, mount: &str) -> bool {
        self.sessions.read().contains_key(mount)
    }

    pub fn ensure_session(&self, mount: &str) {
        let mut sessions = self.sessions.write();
        if !sessions.contains_key(mount) {
            sessions.insert(
                mount.to_string(),
                HlsSession::new(mount.to_string(), self.config.clone()),
            );
        }
    }

    pub fn get_playlist(&self, mount: &str) -> Option<String> {
        let sessions = self.sessions.read();
        sessions.get(mount).map(|s| s.get_playlist())
    }

    pub fn get_segment(&self, mount: &str, sequence: u64) -> Option<Bytes> {
        let sessions = self.sessions.read();
        sessions.get(mount).and_then(|s| s.get_segment(sequence))
    }

    pub fn start(self: Arc<Self>, core: SharedState) -> tokio::task::JoinHandle<()> {
        let config = self.config.clone();
        let sessions = Arc::clone(&self.sessions);

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(config.poll_interval);
            loop {
                interval.tick().await;
                let mut sessions = sessions.write();

                let source_mounts: Vec<String> = core.sources.all_mounts();
                for mount in &source_mounts {
                    if !sessions.contains_key(mount) {
                        sessions.insert(
                            mount.clone(),
                            HlsSession::new(mount.clone(), config.clone()),
                        );
                    }
                }

                let mounts: Vec<String> = sessions.keys().cloned().collect();
                for mount in &mounts {
                    let session = sessions.get_mut(mount).unwrap();

                    if let Some(source) = core.sources.get(mount) {
                        let current_pos = source.buffer.read().current_position();
                        if current_pos > session.last_read_pos {
                            let data = source.buffer.read().read(session.last_read_pos);
                            session.last_read_pos = current_pos;
                            if !data.is_empty() {
                                session.feed(&data);
                            }
                        }
                        if session.should_seal() {
                            session.seal_segment();
                        }
                        if session.is_stale(Duration::from_secs(30)) {
                            session.force_seal();
                        }
                    } else {
                        session.force_seal();
                    }
                }

                let stale_mounts: Vec<String> = sessions
                    .iter()
                    .filter(|(_, s)| {
                        core.sources.get(s.mount()).is_none() && s.is_stale(Duration::from_secs(60))
                    })
                    .map(|(k, _)| k.clone())
                    .collect();
                for mount in stale_mounts {
                    sessions.remove(&mount);
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn session_with_short_duration() -> HlsSession {
        HlsSession::new(
            "/test".into(),
            HlsConfig {
                segment_duration: Duration::from_millis(1),
                window_size: 3,
                poll_interval: Duration::from_millis(100),
            },
        )
    }

    #[test]
    fn seal_segment_noop_with_empty_data() {
        let mut s = session_with_short_duration();
        s.seal_segment();
        assert!(s.segments.is_empty());
        assert_eq!(s.segment_counter, 0);
    }

    #[test]
    fn seal_segment_creates_segment() {
        let mut s = session_with_short_duration();
        s.feed(b"some audio data");
        // feed with short duration triggers auto-seal, but to be safe wait a bit
        std::thread::sleep(Duration::from_millis(2));
        s.feed(b""); // no data, but resets start for next test
        assert_eq!(s.segments.len(), 1);
        assert_eq!(s.segment_counter, 1);
        assert_eq!(s.segments[0].sequence, 0);
        assert!(s.segments[0].duration >= 1.0);
    }

    #[test]
    fn seal_segment_clamps_min_duration() {
        let mut s = session_with_short_duration();
        // call seal directly without waiting — elapsed will be ~0
        s.current_data.extend_from_slice(b"data");
        s.seal_segment();
        assert_eq!(s.segments[0].duration, 1.0);
    }

    #[test]
    fn seal_segment_respects_window_size() {
        let mut s = HlsSession::new(
            "/test".into(),
            HlsConfig {
                segment_duration: Duration::from_secs(10),
                window_size: 3,
                poll_interval: Duration::from_millis(100),
            },
        );
        for i in 0..5 {
            s.current_data.extend_from_slice(b"data");
            s.seal_segment();
            // after i+1 segments, window should never exceed 3
            assert!(s.segments.len() <= 3, "iteration {i}: len {}", s.segments.len());
        }
        assert!(s.segments.len() == 3);
        // with 5 writes and window 3, sequences should be 2, 3, 4
        assert_eq!(s.segments[0].sequence, 2);
        assert_eq!(s.segments[1].sequence, 3);
        assert_eq!(s.segments[2].sequence, 4);
    }

    #[test]
    fn get_playlist_empty() {
        let s = session_with_short_duration();
        let pl = s.get_playlist();
        assert!(pl.contains("#EXTM3U"));
        assert!(pl.contains("#EXT-X-VERSION:3"));
        assert!(pl.contains("#EXT-X-MEDIA-SEQUENCE:0"));
    }

    #[test]
    fn get_playlist_with_segments() {
        let mut s = session_with_short_duration();
        s.current_data.extend_from_slice(b"data");
        s.seal_segment();
        let pl = s.get_playlist();
        assert!(pl.contains("#EXTINF:"));
        assert!(pl.contains("segment-0.ts"));
        assert!(pl.contains("#EXT-X-MEDIA-SEQUENCE:0"));
    }

    #[test]
    fn get_playlist_target_duration() {
        let s = HlsSession::new(
            "/test".into(),
            HlsConfig {
                segment_duration: Duration::from_secs(12),
                window_size: 5,
                poll_interval: Duration::from_millis(100),
            },
        );
        let pl = s.get_playlist();
        assert!(pl.contains("#EXT-X-TARGETDURATION:12"));
    }

    #[test]
    fn should_seal_false_with_no_data() {
        let s = session_with_short_duration();
        assert!(!s.should_seal());
    }

    #[test]
    fn should_seal_true_with_data_and_elapsed() {
        let mut s = session_with_short_duration();
        s.current_data.extend_from_slice(b"data");
        std::thread::sleep(Duration::from_millis(2));
        assert!(s.should_seal());
    }

    #[test]
    fn force_seal_noop_with_empty_data() {
        let mut s = session_with_short_duration();
        s.force_seal();
        assert!(s.segments.is_empty());
    }

    #[test]
    fn force_seal_creates_segment() {
        let mut s = session_with_short_duration();
        s.current_data.extend_from_slice(b"data");
        s.force_seal();
        assert_eq!(s.segments.len(), 1);
    }

    #[test]
    fn has_segment_works() {
        let mut s = session_with_short_duration();
        s.current_data.extend_from_slice(b"data");
        s.seal_segment();
        assert!(s.has_segment(0));
        assert!(!s.has_segment(1));
    }

    #[test]
    fn get_segment_returns_data() {
        let mut s = session_with_short_duration();
        s.current_data.extend_from_slice(b"hello segment");
        s.seal_segment();
        let seg = s.get_segment(0).unwrap();
        assert_eq!(&*seg, b"hello segment");
    }
}
