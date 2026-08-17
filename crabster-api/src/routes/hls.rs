use axum::{
    extract::{Path, State},
    http::{header, StatusCode},
    response::IntoResponse,
    Json,
};
use serde_json::{json, Value};

use crate::SharedApiState;

fn normalize_mount(state: &crate::SharedApiState, mount: &str) -> String {
    if state.core.sources.mount_exists(mount)
        || state.hls.as_ref().map_or(false, |h| h.has_session(mount))
    {
        mount.to_string()
    } else {
        let with_slash = format!("/{}", mount);
        if state.core.sources.mount_exists(&with_slash)
            || state
                .hls
                .as_ref()
                .map_or(false, |h| h.has_session(&with_slash))
        {
            with_slash
        } else {
            mount.to_string()
        }
    }
}

pub async fn get_playlist(
    State(state): State<SharedApiState>,
    Path(mount): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<Value>)> {
    let hls = state.hls.as_ref().ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "HLS not available"})),
        )
    })?;

    let mount = normalize_mount(&state, &mount);
    let playlist = hls.get_playlist(&mount).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "no HLS session for mount"})),
        )
    })?;

    let response = (
        [(header::CONTENT_TYPE, "application/vnd.apple.mpegurl")],
        playlist,
    );
    Ok(response)
}

pub async fn get_segment(
    State(state): State<SharedApiState>,
    Path((mount, sequence_str)): Path<(String, String)>,
) -> Result<impl IntoResponse, (StatusCode, Json<Value>)> {
    let hls = state.hls.as_ref().ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "HLS not available"})),
        )
    })?;

    let mount = normalize_mount(&state, &mount);
    let sequence: u64 = sequence_str.trim_end_matches(".ts").parse().map_err(|_| {
        (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "invalid segment sequence"})),
        )
    })?;

    let data = hls.get_segment(&mount, sequence).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "segment not found"})),
        )
    })?;

    let response = ([(header::CONTENT_TYPE, "video/MP2T")], data);
    Ok(response)
}

pub async fn ensure_hls(
    State(state): State<SharedApiState>,
    Path(mount): Path<String>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let hls = state.hls.as_ref().ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "HLS not available"})),
        )
    })?;

    // Normalize mount name: if source manager stores it with leading /, match that
    let normalized = if state.core.sources.mount_exists(&mount) {
        mount.clone()
    } else {
        let with_slash = format!("/{}", mount);
        if state.core.sources.mount_exists(&with_slash) {
            with_slash
        } else {
            mount.clone()
        }
    };

    hls.ensure_session(&normalized);
    Ok(Json(json!({"status": "ok", "mount": normalized})))
}
