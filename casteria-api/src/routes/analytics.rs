use axum::{
    extract::{Query, State},
    http::StatusCode,
    Json,
};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::SharedApiState;

#[derive(Debug, Deserialize)]
pub struct AnalyticsQuery {
    pub mount: Option<String>,
    pub seconds: Option<i64>,
}

pub async fn get_concurrent(
    State(state): State<SharedApiState>,
    Query(q): Query<AnalyticsQuery>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let analytics = state.analytics.as_ref().ok_or_else(|| {
        (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"error": "analytics not available"})))
    })?;

    let seconds = q.seconds.unwrap_or(300).min(3600);

    let result = if let Some(mount) = &q.mount {
        match analytics.get(mount) {
            Some(a) => {
                let data = a.read().concurrent.recent(seconds);
                json!({ "mount": mount, "data": data, "seconds": seconds })
            }
            None => json!({ "mount": mount, "data": [], "seconds": seconds }),
        }
    } else {
        let mounts: Vec<Value> = analytics
            .all_mounts()
            .iter()
            .map(|a| {
                let a_lock = a.read();
                json!({
                    "mount": a_lock.mount,
                    "data": a_lock.concurrent.recent(seconds)
                })
            })
            .collect();
        json!({ "mounts": mounts, "seconds": seconds })
    };

    Ok(Json(result))
}

pub async fn get_peak(
    State(state): State<SharedApiState>,
    Query(q): Query<AnalyticsQuery>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let analytics = state.analytics.as_ref().ok_or_else(|| {
        (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"error": "analytics not available"})))
    })?;

    let result = if let Some(mount) = &q.mount {
        match analytics.get(mount) {
            Some(a) => {
                let a_lock = a.read();
                json!({
                    "mount": mount,
                    "peak_all_time": a_lock.peak_all_time,
                    "peak_all_time_at": a_lock.peak_all_time_at,
                    "total_connections": a_lock.total_connections
                })
            }
            None => json!({ "mount": mount, "peak_all_time": 0 }),
        }
    } else {
        let mounts: Vec<Value> = analytics
            .all_mounts()
            .iter()
            .map(|a| {
                let a_lock = a.read();
                json!({
                    "mount": a_lock.mount,
                    "peak_all_time": a_lock.peak_all_time,
                    "peak_all_time_at": a_lock.peak_all_time_at
                })
            })
            .collect();
        json!({ "mounts": mounts })
    };

    Ok(Json(result))
}

pub async fn get_devices(
    State(state): State<SharedApiState>,
    Query(q): Query<AnalyticsQuery>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let analytics = state.analytics.as_ref().ok_or_else(|| {
        (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"error": "analytics not available"})))
    })?;

    let result = if let Some(mount) = &q.mount {
        match analytics.get(mount) {
            Some(a) => {
                let devices = a.read().devices.clone();
                let total: u64 = devices.values().sum();
                let breakdown: Vec<Value> = devices
                    .into_iter()
                    .map(|(k, v)| {
                        json!({ "device": k, "count": v, "percentage": if total > 0 { (v as f64 / total as f64 * 100.0).round() } else { 0.0 } })
                    })
                    .collect();
                json!({ "mount": mount, "devices": breakdown })
            }
            None => json!({ "mount": mount, "devices": [] }),
        }
    } else {
        let mut combined: std::collections::HashMap<String, u64> = std::collections::HashMap::new();
        for a in analytics.all_mounts() {
            let a_lock = a.read();
            for (k, v) in &a_lock.devices {
                *combined.entry(k.clone()).or_insert(0) += v;
            }
        }
        let total: u64 = combined.values().sum();
        let breakdown: Vec<Value> = combined
            .into_iter()
            .map(|(k, v)| {
                json!({ "device": k, "count": v, "percentage": if total > 0 { (v as f64 / total as f64 * 100.0).round() } else { 0.0 } })
            })
            .collect();
        json!({ "devices": breakdown })
    };

    Ok(Json(result))
}

pub async fn get_referrers(
    State(state): State<SharedApiState>,
    Query(q): Query<AnalyticsQuery>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let analytics = state.analytics.as_ref().ok_or_else(|| {
        (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"error": "analytics not available"})))
    })?;

    let result = if let Some(mount) = &q.mount {
        match analytics.get(mount) {
            Some(a) => {
                let referrers = a.read().referrers.clone();
                let sorted: Vec<Value> = {
                    let mut pairs: Vec<_> = referrers.into_iter().collect();
                    pairs.sort_by(|a, b| b.1.cmp(&a.1));
                    pairs.into_iter().map(|(k, v)| json!({ "referrer": k, "count": v })).collect()
                };
                json!({ "mount": mount, "referrers": sorted })
            }
            None => json!({ "mount": mount, "referrers": [] }),
        }
    } else {
        let mut combined: std::collections::HashMap<String, u64> = std::collections::HashMap::new();
        for a in analytics.all_mounts() {
            let a_lock = a.read();
            for (k, v) in &a_lock.referrers {
                *combined.entry(k.clone()).or_insert(0) += v;
            }
        }
        let mut sorted: Vec<Value> = combined
            .into_iter()
            .map(|(k, v)| json!({ "referrer": k, "count": v }))
            .collect();
        sorted.sort_by(|a, b| b["count"].as_u64().cmp(&a["count"].as_u64()));
        json!({ "referrers": sorted })
    };

    Ok(Json(result))
}
