pub mod analytics;
pub mod auth;
pub mod health;
pub mod hls;
pub mod tenant;

use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use serde_json::{json, Value};

use crate::models::*;
use crate::SharedApiState;

fn source_to_mount(source: &casteria_core::source::Source) -> MountResponse {
    let (metadata, current_listeners, peak_listeners, max_listeners, icy_br) = {
        let meta = source.info.metadata.read();
        let stats = source.info.stats.read();
        (
            StreamMetadataResponse {
                title: meta.title.clone(),
                artist: meta.artist.clone(),
                song: meta.song.clone(),
                description: meta.description.clone(),
                genre: meta.genre.clone(),
                url: meta.url.clone(),
                icy_name: meta.icy_name.clone(),
                icy_genre: meta.icy_genre.clone(),
                icy_url: meta.icy_url.clone(),
                icy_br: meta.icy_br,
            },
            stats.current_listeners,
            stats.peak_listeners,
            stats.max_listeners,
            meta.icy_br,
        )
    };

    MountResponse {
        mount: source.info.mount.clone(),
        source_connected: source.connected.load(std::sync::atomic::Ordering::Relaxed),
        format: format!("{:?}", source.info.format),
        bitrate: icy_br,
        sample_rate: source.info.sample_rate,
        channels: source.info.channels,
        current_listeners,
        peak_listeners,
        max_listeners,
        public: source.info.public,
        hidden: source.info.hidden,
        connected_at: None,
        metadata,
        audio_info: source.info.audio_info.clone(),
    }
}

pub async fn get_status(
    State(state): State<SharedApiState>,
) -> Result<Json<ApiStatus>, (StatusCode, Json<Value>)> {
    let core = &state.core;
    let global = core.stats.global();
    Ok(Json(ApiStatus {
        version: env!("CARGO_PKG_VERSION").to_string(),
        uptime_seconds: global.started_at.elapsed().as_secs(),
        sources_active: core.sources.count(),
        listeners_total: global
            .current_listeners
            .load(std::sync::atomic::Ordering::Relaxed),
        bytes_sent: global
            .total_bytes_sent
            .load(std::sync::atomic::Ordering::Relaxed),
        bytes_received: global
            .total_bytes_received
            .load(std::sync::atomic::Ordering::Relaxed),
    }))
}

pub async fn get_mounts(
    State(state): State<SharedApiState>,
) -> Result<Json<Vec<MountResponse>>, (StatusCode, Json<Value>)> {
    let mounts = state.core.sources.all_sources();
    let responses: Vec<_> = mounts.iter().map(|s| source_to_mount(s)).collect();
    Ok(Json(responses))
}

pub async fn get_mount(
    State(state): State<SharedApiState>,
    Path(mount): Path<String>,
) -> Result<Json<MountResponse>, (StatusCode, Json<Value>)> {
    let source = state.core.sources.get(&mount).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "mount not found"})),
        )
    })?;
    Ok(Json(source_to_mount(&source)))
}

pub async fn get_mount_listeners(
    State(state): State<SharedApiState>,
    Path(mount): Path<String>,
) -> Result<Json<ListenerSummary>, (StatusCode, Json<Value>)> {
    let source = state.core.sources.get(&mount).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "mount not found"})),
        )
    })?;

    let listener_count = source.info.stats.read().current_listeners;

    Ok(Json(ListenerSummary {
        mount: mount.clone(),
        listeners: Vec::new(),
        total: listener_count as usize,
    }))
}

pub async fn get_stats(
    State(state): State<SharedApiState>,
) -> Result<Json<StatsResponse>, (StatusCode, Json<Value>)> {
    let core = &state.core;
    let global = core.stats.global();
    let sources = core.sources.all_sources();
    let mounts: Vec<_> = sources.iter().map(|s| source_to_mount(s)).collect();

    Ok(Json(StatsResponse {
        server_started: String::new(),
        sources_active: core.sources.count(),
        listeners_total: global
            .current_listeners
            .load(std::sync::atomic::Ordering::Relaxed),
        peak_listeners: global
            .peak_listeners
            .load(std::sync::atomic::Ordering::Relaxed),
        bytes_sent: global
            .total_bytes_sent
            .load(std::sync::atomic::Ordering::Relaxed),
        bytes_received: global
            .total_bytes_received
            .load(std::sync::atomic::Ordering::Relaxed),
        total_connections: global
            .total_connections
            .load(std::sync::atomic::Ordering::Relaxed),
        total_source_connections: global
            .total_source_connections
            .load(std::sync::atomic::Ordering::Relaxed),
        mounts,
    }))
}

pub async fn get_sources(
    State(state): State<SharedApiState>,
) -> Result<Json<Vec<SourceResponse>>, (StatusCode, Json<Value>)> {
    let sources = state.core.sources.all_sources();
    let mut responses = Vec::new();

    for source in sources {
        let bitrate = source.info.metadata.read().icy_br;
        responses.push(SourceResponse {
            mount: source.info.mount.clone(),
            connected: source.connected.load(std::sync::atomic::Ordering::Relaxed),
            ip: source.info.client_ip.clone(),
            user_agent: source.info.user_agent.clone(),
            format: format!("{:?}", source.info.format),
            connected_at: String::new(),
            bytes_received: source.info.stats.read().total_bytes_received,
            bitrate,
            audio_info: source.info.audio_info.clone(),
        });
    }

    Ok(Json(responses))
}
