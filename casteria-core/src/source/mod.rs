use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use dashmap::DashMap;
use parking_lot::RwLock as PLRwLock;
use uuid::Uuid;

use crate::format::FormatType;
use crate::stats::MountStats;

#[derive(Debug, Clone)]
pub struct SourceInfo {
    pub id: Uuid,
    pub mount: String,
    pub connected_at: Instant,
    pub client_ip: String,
    pub user_agent: String,
    pub format: FormatType,
    pub audio_info: HashMap<String, String>,
    pub bitrate: Option<u32>,
    pub quality: Option<f32>,
    pub channels: Option<u8>,
    pub sample_rate: Option<u32>,
    pub max_listeners: Option<usize>,
    pub public: bool,
    pub hidden: bool,
    pub fallback_mount: Option<String>,
    pub fallback_override: bool,
    pub fallback_when_full: bool,
    pub burst_size: usize,
    pub metadata: Arc<PLRwLock<StreamMetadata>>,
    pub stats: Arc<PLRwLock<MountStats>>,
}

#[derive(Debug, Clone)]
pub struct StreamMetadata {
    pub title: Option<String>,
    pub artist: Option<String>,
    pub song: Option<String>,
    pub description: Option<String>,
    pub genre: Option<String>,
    pub url: Option<String>,
    pub icy_name: Option<String>,
    pub icy_genre: Option<String>,
    pub icy_url: Option<String>,
    pub icy_br: Option<u32>,
    pub audio_info: HashMap<String, String>,
}

impl Default for StreamMetadata {
    fn default() -> Self {
        Self {
            title: None,
            artist: None,
            song: None,
            description: None,
            genre: None,
            url: None,
            icy_name: None,
            icy_genre: None,
            icy_url: None,
            icy_br: None,
            audio_info: HashMap::new(),
        }
    }
}

pub struct RingBuffer {
    buffer: Vec<u8>,
    capacity: usize,
    write_pos: AtomicU64,
}

impl RingBuffer {
    pub fn new(capacity: usize) -> Self {
        Self {
            buffer: vec![0u8; capacity],
            capacity,
            write_pos: AtomicU64::new(0),
        }
    }

    pub fn write(&mut self, data: &[u8]) {
        let len = data.len();
        if len >= self.capacity {
            let offset = len - self.capacity;
            let tail = &data[offset..];
            self.buffer.copy_from_slice(tail);
            self.write_pos.fetch_add(len as u64, Ordering::AcqRel);
            return;
        }

        let pos = self.write_pos.load(Ordering::Acquire) as usize % self.capacity;
        let end = pos + len;
        if end <= self.capacity {
            self.buffer[pos..end].copy_from_slice(data);
        } else {
            let first = self.capacity - pos;
            self.buffer[pos..].copy_from_slice(&data[..first]);
            self.buffer[..end % self.capacity].copy_from_slice(&data[first..]);
        }
        self.write_pos.fetch_add(len as u64, Ordering::AcqRel);
    }

    pub fn read(&self, offset: u64) -> Bytes {
        let write_pos = self.write_pos.load(Ordering::Acquire);
        let readable = (write_pos - offset) as usize;
        if readable == 0 {
            return Bytes::new();
        }
        let len = readable.min(self.capacity);
        let start = (offset as usize) % self.capacity;
        if start + len <= self.capacity {
            Bytes::copy_from_slice(&self.buffer[start..start + len])
        } else {
            let first = self.capacity - start;
            let mut buf = Vec::with_capacity(len);
            buf.extend_from_slice(&self.buffer[start..]);
            buf.extend_from_slice(&self.buffer[..len - first]);
            Bytes::from(buf)
        }
    }

    pub fn current_position(&self) -> u64 {
        self.write_pos.load(Ordering::Acquire)
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

pub struct Source {
    pub info: Arc<SourceInfo>,
    pub buffer: Arc<PLRwLock<RingBuffer>>,
    pub connected: AtomicBool,
    pub running: AtomicBool,
}

pub struct SourceManager {
    sources: DashMap<String, Arc<Source>>,
}

impl SourceManager {
    pub fn new() -> Self {
        Self {
            sources: DashMap::new(),
        }
    }

    pub fn register(&self, mount: String, source: Arc<Source>) -> bool {
        if self.sources.contains_key(&mount) {
            return false;
        }
        self.sources.insert(mount, source);
        true
    }

    pub fn unregister(&self, mount: &str) -> Option<Arc<Source>> {
        self.sources.remove(mount).map(|(_, v)| v)
    }

    pub fn get(&self, mount: &str) -> Option<Arc<Source>> {
        self.sources.get(mount).map(|r| Arc::clone(&r))
    }

    pub fn get_source_info(&self, mount: &str) -> Option<Arc<SourceInfo>> {
        self.sources.get(mount).map(|r| Arc::clone(&r.info))
    }

    pub fn mount_exists(&self, mount: &str) -> bool {
        self.sources.contains_key(mount)
    }

    pub fn all_mounts(&self) -> Vec<String> {
        self.sources.iter().map(|r| r.key().clone()).collect()
    }

    pub fn all_sources(&self) -> Vec<Arc<Source>> {
        self.sources
            .iter()
            .map(|r| Arc::clone(&r.value()))
            .collect()
    }

    pub fn count(&self) -> usize {
        self.sources.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ring_buffer_write_read_normal() {
        let mut rb = RingBuffer::new(16);
        rb.write(b"hello");
        let data = rb.read(0);
        assert_eq!(&*data, b"hello");
    }

    #[test]
    fn ring_buffer_write_exact_capacity() {
        let mut rb = RingBuffer::new(8);
        rb.write(b"12345678");
        let data = rb.read(0);
        assert_eq!(&*data, b"12345678");
    }

    #[test]
    fn ring_buffer_write_exceeds_capacity() {
        let mut rb = RingBuffer::new(8);
        rb.write(b"0123456789ABCDEF"); // 16 bytes, twice capacity
        let data = rb.read(0);
        // only the last 8 bytes should remain
        assert_eq!(&*data, b"89ABCDEF");
    }

    #[test]
    fn ring_buffer_write_wraps_around() {
        let mut rb = RingBuffer::new(8);
        // write 6 bytes at pos 0..6: buffer = [a,b,c,d,e,f,_,_]
        rb.write(b"abcdef");
        // next write of 4 bytes: pos=6, wraps: buffer[6..8]="12", buffer[0..2]="34"
        // buffer becomes: [3,4,c,d,e,f,1,2]
        rb.write(b"1234");
        let data = rb.read(0);
        // readable = 10, capped at capacity 8, data = buffer[0..8]
        assert_eq!(&*data, b"34cdef12");
    }

    #[test]
    fn ring_buffer_read_wraps_around() {
        let mut rb = RingBuffer::new(8);
        // fill buffer then overwrite partially to create wrap
        rb.write(b"01234567"); // 8 bytes, fills exactly
        rb.write(b"ABCD");    // 4 bytes, wraps: pos 0..4
        // Read from position 4 (where "4567ABCD" starts in ring)
        let data = rb.read(4);
        assert_eq!(&*data, b"4567ABCD");
    }

    #[test]
    fn ring_buffer_read_at_offset() {
        let mut rb = RingBuffer::new(16);
        rb.write(b"abcdefghij");
        let data = rb.read(3);
        assert_eq!(&*data, b"defghij");
    }

    #[test]
    fn ring_buffer_read_nothing_available() {
        let rb = RingBuffer::new(16);
        let data = rb.read(0);
        assert!(data.is_empty());
    }

    #[test]
    fn ring_buffer_current_position() {
        let mut rb = RingBuffer::new(16);
        assert_eq!(rb.current_position(), 0);
        rb.write(b"hello");
        assert_eq!(rb.current_position(), 5);
        rb.write(b"world");
        assert_eq!(rb.current_position(), 10);
    }

    #[test]
    fn ring_buffer_capacity() {
        let rb = RingBuffer::new(64);
        assert_eq!(rb.capacity(), 64);
    }

    #[test]
    fn ring_buffer_zero_len_write() {
        let mut rb = RingBuffer::new(16);
        rb.write(b"");
        let data = rb.read(0);
        assert!(data.is_empty());
        assert_eq!(rb.current_position(), 0);
    }
}
