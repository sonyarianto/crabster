use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

pub const RING_CAPACITY: usize = 300;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeSeriesPoint {
    pub timestamp: DateTime<Utc>,
    pub value: f64,
}

pub struct TimeSeriesRingBuffer {
    points: Vec<TimeSeriesPoint>,
    capacity: usize,
    next_index: usize,
    count: usize,
}

impl TimeSeriesRingBuffer {
    pub fn new(capacity: usize) -> Self {
        Self {
            points: Vec::with_capacity(capacity),
            capacity,
            next_index: 0,
            count: 0,
        }
    }

    pub fn record(&mut self, value: impl Into<f64>) {
        let point = TimeSeriesPoint {
            timestamp: Utc::now(),
            value: value.into(),
        };
        if self.count < self.capacity {
            self.points.push(point);
            self.count += 1;
        } else {
            self.points[self.next_index] = point;
        }
        self.next_index = (self.next_index + 1) % self.capacity;
    }

    pub fn points(&self) -> &[TimeSeriesPoint] {
        if self.count < self.capacity {
            &self.points[..self.count]
        } else {
            &self.points
        }
    }

    pub fn recent(&self, seconds: i64) -> Vec<TimeSeriesPoint> {
        let cutoff = Utc::now() - chrono::Duration::seconds(seconds);
        self.points()
            .iter()
            .filter(|p| p.timestamp >= cutoff)
            .cloned()
            .collect()
    }

    pub fn count(&self) -> usize {
        self.count.min(self.capacity)
    }
}

pub struct MountAnalytics {
    pub mount: String,
    pub concurrent: TimeSeriesRingBuffer,
    pub peak_all_time: u64,
    pub peak_all_time_at: Option<DateTime<Utc>>,
    pub total_connections: u64,
    pub total_listener_seconds: u64,
    pub total_sessions: u64,
    pub devices: HashMap<String, u64>,
    pub referrers: HashMap<String, u64>,
    pub created_at: DateTime<Utc>,
}

impl MountAnalytics {
    pub fn new(mount: &str) -> Self {
        Self {
            mount: mount.to_string(),
            concurrent: TimeSeriesRingBuffer::new(RING_CAPACITY),
            peak_all_time: 0,
            peak_all_time_at: None,
            total_connections: 0,
            total_listener_seconds: 0,
            total_sessions: 0,
            devices: HashMap::new(),
            referrers: HashMap::new(),
            created_at: Utc::now(),
        }
    }
}
