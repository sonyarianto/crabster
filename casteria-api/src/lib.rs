pub mod middleware;
pub mod models;
pub mod routes;

use axum::routing::{get, post};
use axum::Router;
use casteria_core::SharedState;
use std::sync::Arc;
use tower_http::cors::{Any, CorsLayer};

pub struct ApiState {
    pub core: SharedState,
    pub db: Option<casteria_db::Database>,
    pub jwt_secret: String,
    pub analytics: Option<std::sync::Arc<casteria_analytics::AnalyticsCollector>>,
    pub health: Option<Arc<casteria_health::checker::HealthChecker>>,
    pub hls: Option<Arc<casteria_hls::HlsManager>>,
}

pub type SharedApiState = Arc<ApiState>;

pub fn create_api_router(state: SharedApiState) -> Router {
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    let api_routes = Router::new()
        .route("/status", get(routes::get_status))
        .route("/mounts", get(routes::get_mounts))
        .route("/mounts/{mount}", get(routes::get_mount))
        .route(
            "/mounts/{mount}/listeners",
            get(routes::get_mount_listeners),
        )
        .route("/sources", get(routes::get_sources))
        .route("/stats", get(routes::get_stats))
        .route("/auth/login", post(routes::auth::login))
        .route("/auth/verify", get(routes::auth::verify));

    let analytics_routes = Router::new()
        .route(
            "/analytics/concurrent",
            get(routes::analytics::get_concurrent),
        )
        .route("/analytics/peak", get(routes::analytics::get_peak))
        .route("/analytics/devices", get(routes::analytics::get_devices))
        .route(
            "/analytics/referrers",
            get(routes::analytics::get_referrers),
        );

    let health_routes = Router::new()
        .route("/health", get(routes::health::get_health_status))
        .route("/health/alerts", get(routes::health::get_alerts))
        .route(
            "/health/alerts/{id}/acknowledge",
            post(routes::health::acknowledge_alert),
        );

    let hls_routes = Router::new()
        .route("/hls/{mount}/playlist.m3u8", get(routes::hls::get_playlist))
        .route(
            "/hls/{mount}/segment/{segment}",
            get(routes::hls::get_segment),
        )
        .route("/hls/{mount}/start", get(routes::hls::ensure_hls));

    let admin_routes = Router::new()
        .route("/accounts", get(routes::tenant::list_accounts))
        .route("/accounts", post(routes::tenant::create_account))
        .route("/accounts/{id}", get(routes::tenant::get_account));

    Router::new()
        .nest("/api/v1", api_routes)
        .nest("/api/v1", analytics_routes)
        .nest("/api/v1", health_routes)
        .nest("/api/v1", hls_routes)
        .nest("/api/v1/admin", admin_routes)
        .layer(cors)
        .with_state(state)
}
