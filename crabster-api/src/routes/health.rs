use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use serde_json::{json, Value};
use uuid::Uuid;

use crate::SharedApiState;

pub async fn get_health_status(
    State(state): State<SharedApiState>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let health_checker = state.health.as_ref().ok_or_else(|| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "health monitoring not available"})),
        )
    })?;

    let core = &state.core;
    let statuses = health_checker.check(core);

    let mounts: Vec<Value> = statuses
        .iter()
        .map(|s| {
            let level = match s.overall {
                crabster_health::checker::HealthLevel::Green => "green",
                crabster_health::checker::HealthLevel::Yellow => "yellow",
                crabster_health::checker::HealthLevel::Red => "red",
            };
            json!({
                "mount": s.mount,
                "source_connected": s.source_connected,
                "bitrate": s.bitrate,
                "bitrate_healthy": s.bitrate_healthy,
                "metadata_fresh": s.metadata_fresh,
                "listener_count": s.listener_count,
                "listener_trend": s.listener_trend,
                "buffer_backlog": s.buffer_backlog,
                "health": level,
            })
        })
        .collect();

    let red = mounts.iter().filter(|m| m["health"] == "red").count();
    let yellow = mounts.iter().filter(|m| m["health"] == "yellow").count();
    let green = mounts.iter().filter(|m| m["health"] == "green").count();

    Ok(Json(json!({
        "status": if red > 0 { "degraded" } else if yellow > 0 { "warning" } else { "healthy" },
        "summary": { "green": green, "yellow": yellow, "red": red },
        "mounts": mounts,
    })))
}

pub async fn get_alerts(
    State(state): State<SharedApiState>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let health_checker = state.health.as_ref().ok_or_else(|| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "health monitoring not available"})),
        )
    })?;

    let alerts = health_checker.alerts();
    Ok(Json(json!({ "alerts": alerts })))
}

pub async fn acknowledge_alert(
    State(state): State<SharedApiState>,
    Path(id): Path<String>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let health_checker = state.health.as_ref().ok_or_else(|| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "health monitoring not available"})),
        )
    })?;

    let id = Uuid::parse_str(&id).map_err(|_| {
        (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "invalid alert id"})),
        )
    })?;

    health_checker.alert_engine().acknowledge(&id);
    Ok(Json(json!({"status": "acknowledged"})))
}
