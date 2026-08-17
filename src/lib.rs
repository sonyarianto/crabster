use std::sync::Arc;

use anyhow::Result;
use base64::Engine;
use crabster_cluster::{ClusterConfig, ClusterMode};
use crabster_core::config::Config;
use crabster_core::source::{RingBuffer, Source, SourceInfo, StreamMetadata};
use crabster_core::{AppState, SharedState};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock;
use tracing::{error, info, warn};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ServerConfig {
    pub stream_port: u16,
    pub api_port: u16,
    pub cluster_port: u16,
    pub cluster_enabled: bool,
    pub cluster_mode: ClusterMode,
    pub db_path: Option<String>,
    pub jwt_secret: String,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            stream_port: 8000,
            api_port: 8001,
            cluster_port: 8002,
            cluster_enabled: true,
            cluster_mode: ClusterMode::Origin,
            db_path: Some("crabster.db".into()),
            jwt_secret: "crabster-jwt-secret-change-me-please".into(),
        }
    }
}

pub async fn run_server(config: ServerConfig) -> Result<()> {
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        let _ = shutdown_tx.send(());
    });
    run_with_config(config, shutdown_rx).await
}

pub async fn run_with_config(
    config: ServerConfig,
    shutdown_rx: tokio::sync::oneshot::Receiver<()>,
) -> Result<()> {
    let db_path = config.db_path.as_deref().unwrap_or("crabster.db");

    let db = match crabster_db::Database::open(db_path) {
        Ok(db) => {
            crabster_db::auth::register_default_admin(&db)?;
            info!("Database ready at {}", db_path);
            Some(db)
        }
        Err(e) => {
            warn!(
                "Database unavailable ({}), running without multi-tenant support",
                e
            );
            None
        }
    };

    let core_config = Config::default();

    let core_state: SharedState = Arc::new(AppState {
        config: RwLock::new(core_config.clone()),
        sources: crabster_core::source::SourceManager::new(),
        stats: crabster_core::stats::StatsCollector::new(),
        format_registry: crabster_core::format::FormatRegistry::new(),
    });

    let analytics = Arc::new(crabster_analytics::AnalyticsCollector::new());
    let _analytics_handle = crabster_analytics::collector::start_collector(
        Arc::clone(&analytics),
        Arc::clone(&core_state),
    );

    let health_checker = Arc::new(crabster_health::checker::HealthChecker::new(Arc::new(
        crabster_health::alerts::AlertEngine::new(1000),
    )));
    let _health_handle = health_checker.clone().start(Arc::clone(&core_state));

    let hls_manager = Arc::new(crabster_hls::HlsManager::new(
        crabster_hls::HlsConfig::default(),
    ));
    let _hls_handle = hls_manager.clone().start(Arc::clone(&core_state));

    let api_state: crabster_api::SharedApiState = Arc::new(crabster_api::ApiState {
        core: Arc::clone(&core_state),
        db: db.clone(),
        jwt_secret: config.jwt_secret,
        analytics: Some(analytics),
        health: Some(health_checker),
        hls: Some(hls_manager),
    });

    info!("Crabster v{} starting...", env!("CARGO_PKG_VERSION"));

    let mut handles = Vec::new();

    // Start REST API server
    let api_state_clone = Arc::clone(&api_state);
    let api_addr = format!("0.0.0.0:{}", config.api_port);
    let api_handle = tokio::spawn(async move {
        let app = crabster_api::create_api_router(api_state_clone);
        info!("REST API listening on {}", api_addr);
        let listener = TcpListener::bind(&api_addr).await.unwrap();
        if let Err(e) = axum::serve(listener, app).await {
            warn!("REST API server error: {}", e);
        }
    });
    handles.push(api_handle);

    // Start streaming listener
    let stream_addr = format!("0.0.0.0:{}", config.stream_port);
    let state_for_stream = Arc::clone(&core_state);
    let db_for_stream = db.clone();
    let handle = tokio::spawn(async move {
        let listener = TcpListener::bind(&stream_addr).await.unwrap();
        info!("Streaming server listening on {}", stream_addr);
        loop {
            match listener.accept().await {
                Ok((stream, peer_addr)) => {
                    let state = Arc::clone(&state_for_stream);
                    let db = db_for_stream.clone();
                    tokio::spawn(async move {
                        if let Err(e) = handle_connection(stream, state, db).await {
                            error!("Connection error from {}: {}", peer_addr, e);
                        }
                    });
                }
                Err(e) => {
                    error!("Accept error: {}", e);
                }
            }
        }
    });
    handles.push(handle);

    // Start cluster
    if config.cluster_enabled {
        match config.cluster_mode {
            ClusterMode::Origin => {
                let origin = Arc::new(crabster_cluster::origin::OriginNode::new());
                let origin_core = Arc::clone(&core_state);
                let origin_config = ClusterConfig {
                    origin_port: config.cluster_port,
                    ..Default::default()
                };
                let origin_handle = tokio::spawn(async move {
                    origin.start(&origin_config, origin_core).await;
                });
                handles.push(origin_handle);
                info!("Cluster mode: Origin (relay port {})", config.cluster_port);
            }
            ClusterMode::Edge => {
                let edge = crabster_cluster::edge::EdgeNode::new();
                let edge_core = Arc::clone(&core_state);
                let edge_config = ClusterConfig {
                    origin_port: config.cluster_port,
                    ..Default::default()
                };
                let edge_handle = tokio::spawn(async move {
                    edge.start(&edge_config, edge_core).await;
                });
                handles.push(edge_handle);
                info!("Cluster mode: Edge");
            }
            ClusterMode::Standalone => {
                info!("Cluster mode: Standalone");
            }
        }
    }

    info!("Crabster ready.");
    shutdown_rx.await.ok();
    info!("Shutting down...");
    Ok(())
}

async fn handle_connection(
    stream: TcpStream,
    state: SharedState,
    db: Option<crabster_db::Database>,
) -> Result<()> {
    let (reader, mut writer) = tokio::io::split(stream);
    let mut buf_reader = BufReader::new(reader);
    let mut request_line = String::new();
    buf_reader.read_line(&mut request_line).await?;

    if request_line.is_empty() {
        return Ok(());
    }

    let parts: Vec<&str> = request_line.split_whitespace().collect();
    if parts.len() < 2 {
        return Ok(());
    }

    let method = parts[0];
    let path = parts[1];

    let mut headers = Vec::new();
    loop {
        let mut line = String::new();
        buf_reader.read_line(&mut line).await?;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            break;
        }
        if let Some((key, value)) = trimmed.split_once(':') {
            headers.push((key.trim().to_string(), value.trim().to_string()));
        }
    }

    let headers_map: std::collections::HashMap<String, String> = headers
        .iter()
        .map(|(k, v)| (k.to_lowercase(), v.clone()))
        .collect();

    match method.to_uppercase().as_str() {
        "SOURCE" => handle_source(path, &headers_map, buf_reader, writer, state, db).await,
        "PUT" => handle_source(path, &headers_map, buf_reader, writer, state, db).await,
        "GET" | "HEAD" => handle_get(path, &headers_map, writer, state).await,
        "POST" => handle_post(path, &headers_map, buf_reader, writer, state).await,
        _ => {
            let _ = writer
                .write_all(b"HTTP/1.0 501 Not Implemented\r\n\r\n")
                .await;
        }
    }

    Ok(())
}

fn extract_basic_auth(
    headers: &std::collections::HashMap<String, String>,
) -> Option<(String, String)> {
    headers.get("authorization").and_then(|auth| {
        if auth.starts_with("Basic ") {
            let encoded = auth.trim_start_matches("Basic ");
            if let Ok(decoded) = base64::engine::general_purpose::STANDARD.decode(encoded) {
                let decoded_str = String::from_utf8_lossy(&decoded);
                let mut parts = decoded_str.splitn(2, ':');
                let user = parts.next()?.to_string();
                let pass = parts.next()?.to_string();
                return Some((user, pass));
            }
        }
        None
    })
}

async fn handle_source(
    mount: &str,
    headers: &std::collections::HashMap<String, String>,
    mut reader: BufReader<tokio::io::ReadHalf<tokio::net::TcpStream>>,
    mut writer: tokio::io::WriteHalf<tokio::net::TcpStream>,
    state: SharedState,
    db: Option<crabster_db::Database>,
) {
    let mount = mount.split('?').next().unwrap_or(mount);

    if state.sources.mount_exists(mount) {
        let _ = writer
            .write_all(b"HTTP/1.0 403 Forbidden\r\nContent-Type: text/plain\r\n\r\nMountpoint already in use\r\n")
            .await;
        return;
    }

    let (username, password) = match extract_basic_auth(headers) {
        Some(creds) => creds,
        None => {
            let _ = writer
                .write_all(b"HTTP/1.0 401 Unauthorized\r\nWWW-Authenticate: Basic realm=\"crabster\"\r\nContent-Length: 0\r\n\r\n")
                .await;
            return;
        }
    };

    let valid = if let Some(ref db) = db {
        match db.get_mount_config(mount) {
            Ok(Some(config)) => match db.check_account_quota(&config.account_id) {
                Ok((true, _)) => password == config.source_password,
                Ok((false, msg)) => {
                    warn!("Quota exceeded for {}: {}", mount, msg);
                    let _ = writer
                        .write_all(format!("HTTP/1.0 403 Forbidden\r\n\r\n{}\r\n", msg).as_bytes())
                        .await;
                    return;
                }
                Err(e) => {
                    warn!("Quota check error: {}", e);
                    false
                }
            },
            Ok(None) => {
                let config = state.config.read().await;
                password == config.authentication.source_password && username == "source"
            }
            Err(e) => {
                warn!("DB error checking mount: {}", e);
                false
            }
        }
    } else {
        let config = state.config.read().await;
        username == "source" && password == config.authentication.source_password
    };

    if !valid {
        let _ = writer
            .write_all(b"HTTP/1.0 401 Unauthorized\r\nWWW-Authenticate: Basic realm=\"crabster\"\r\nContent-Length: 0\r\n\r\n")
            .await;
        return;
    }

    let content_type = headers
        .get("content-type")
        .cloned()
        .unwrap_or_else(|| "application/octet-stream".into());

    let user_agent = headers.get("user-agent").cloned().unwrap_or_default();

    let format_type = crabster_core::format::FormatType::from_content_type(&content_type);
    let mount_stats = state.stats.ensure_mount(mount);

    let info = Arc::new(SourceInfo {
        id: uuid::Uuid::new_v4(),
        mount: mount.to_string(),
        connected_at: std::time::Instant::now(),
        client_ip: String::new(),
        user_agent,
        format: format_type,
        audio_info: std::collections::HashMap::new(),
        bitrate: None,
        quality: None,
        channels: None,
        sample_rate: None,
        max_listeners: None,
        public: true,
        hidden: false,
        fallback_mount: None,
        fallback_override: false,
        fallback_when_full: false,
        burst_size: 65536,
        metadata: Arc::new(parking_lot::RwLock::new(StreamMetadata::default())),
        stats: mount_stats,
    });

    let buffer = Arc::new(parking_lot::RwLock::new(RingBuffer::new(65536)));

    let source = Arc::new(Source {
        info: Arc::clone(&info),
        buffer: Arc::clone(&buffer),
        connected: std::sync::atomic::AtomicBool::new(true),
        running: std::sync::atomic::AtomicBool::new(true),
    });

    if !state
        .sources
        .register(mount.to_string(), Arc::clone(&source))
    {
        let _ = writer
            .write_all(b"HTTP/1.0 503 Service Unavailable\r\n\r\n")
            .await;
        return;
    }

    let _ = writer
        .write_all(format!("HTTP/1.0 200 OK\r\nContent-Type: {}\r\n\r\n", content_type).as_bytes())
        .await;

    info!("Source connected: {} ({})", mount, content_type);

    let mut buf = vec![0u8; 8192];
    loop {
        match reader.read(&mut buf).await {
            Ok(0) => break,
            Ok(n) => {
                let data = &buf[..n];
                buffer.write().write(data);
                state
                    .stats
                    .global()
                    .total_bytes_received
                    .fetch_add(n as u64, std::sync::atomic::Ordering::Relaxed);
            }
            Err(_) => break,
        }
    }

    source
        .connected
        .store(false, std::sync::atomic::Ordering::Relaxed);
    source
        .running
        .store(false, std::sync::atomic::Ordering::Relaxed);
    state.sources.unregister(mount);
    info!("Source disconnected: {}", mount);
}

async fn handle_get(
    path: &str,
    _headers: &std::collections::HashMap<String, String>,
    mut writer: tokio::io::WriteHalf<tokio::net::TcpStream>,
    state: SharedState,
) {
    let path = path.split('?').next().unwrap_or(path);

    if path == "/" || path == "/status.xsl" || path == "/status-json.xsl" {
        let xml = r#"<?xml version="1.0"?>
<icestats>
  <admin>crabster</admin>
  <host>localhost</host>
  <location>Earth</location>
  <server_id>Crabster/0.1.0</server_id>
  <server_start>0</server_start>
  <source_total>0</source_total>
  <sources>0</sources>
  <listeners>0</listeners>
  <listener_connections>0</listener_connections>
</icestats>"#;
        let body = if path == "/status-json.xsl" {
            xml.to_string()
        } else {
            format!(
                "<html><body><h1>Crabster</h1><pre>{}</pre></body></html>",
                xml
            )
        };
        let content_type = if path == "/status-json.xsl" {
            "application/json"
        } else {
            "text/html"
        };
        let _ = writer
            .write_all(
                format!(
                    "HTTP/1.0 200 OK\r\nContent-Type: {}\r\nContent-Length: {}\r\n\r\n{}",
                    content_type,
                    body.len(),
                    body
                )
                .as_bytes(),
            )
            .await;
        return;
    }

    if path.starts_with("/admin/") {
        handle_legacy_admin(path, writer, state).await;
        return;
    }

    let mount = path;
    let source = match state.sources.get(mount) {
        Some(s) => s,
        None => {
            let _ = writer
                .write_all(b"HTTP/1.0 404 Not Found\r\nContent-Type: text/plain\r\n\r\nNo such mountpoint\r\n")
                .await;
            return;
        }
    };

    let format_info = state.format_registry.get(source.info.format).unwrap();

    let (icy_br, icy_name, icy_genre, icy_url, icy_pub, icy_meta_interval) = {
        let meta = source.info.metadata.read();
        (
            meta.icy_br.unwrap_or(128),
            meta.icy_name
                .clone()
                .unwrap_or_else(|| "Crabster Stream".into()),
            meta.icy_genre.clone().unwrap_or_else(|| "Various".into()),
            meta.icy_url.clone().unwrap_or_default(),
            if source.info.public { "1" } else { "0" },
            format_info.icy_metadata_interval,
        )
    };

    let mut response = format!(
        "HTTP/1.0 200 OK\r\nContent-Type: {}\r\n\
         icy-br: {}\r\n\
         icy-name: {}\r\n\
         icy-genre: {}\r\n\
         icy-url: {}\r\n\
         icy-pub: {}\r\n",
        format_info.content_type, icy_br, icy_name, icy_genre, icy_url, icy_pub,
    );

    if let Some(interval) = icy_meta_interval {
        response.push_str(&format!("icy-metaint: {}\r\n", interval));
    }

    response.push_str(
        "Access-Control-Allow-Origin: *\r\n\
         Cache-Control: no-cache\r\n\
         Pragma: no-cache\r\n\
         Expires: 0\r\n\r\n",
    );

    if let Err(_) = writer.write_all(response.as_bytes()).await {
        return;
    }

    let buf_clone = Arc::clone(&source.buffer);
    let mut pos = buf_clone.read().current_position();

    loop {
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        if !source.connected.load(std::sync::atomic::Ordering::Relaxed) {
            break;
        }

        let current_pos = buf_clone.read().current_position();
        if current_pos > pos {
            let data = buf_clone.read().read(pos);
            pos = current_pos;
            if !data.is_empty() {
                if let Err(_) = writer.write_all(&data).await {
                    break;
                }
            }
        }
    }
}

async fn handle_post(
    path: &str,
    _headers: &std::collections::HashMap<String, String>,
    _reader: BufReader<tokio::io::ReadHalf<tokio::net::TcpStream>>,
    mut writer: tokio::io::WriteHalf<tokio::net::TcpStream>,
    state: SharedState,
) {
    let path = path.split('?').next().unwrap_or(path);
    if path.starts_with("/admin/") {
        handle_legacy_admin(path, writer, state).await;
        return;
    }
    let _ = writer
        .write_all(b"HTTP/1.0 405 Method Not Allowed\r\n\r\n")
        .await;
}

async fn handle_legacy_admin(
    path: &str,
    mut writer: tokio::io::WriteHalf<tokio::net::TcpStream>,
    state: SharedState,
) {
    let path = path.trim_start_matches("/admin/");
    let path = path.split('?').next().unwrap_or(path);

    let cmd = crabster_core::admin::AdminCommand::from_path(path);
    let response = match cmd {
        Some(command) => crabster_core::admin::handle_admin_command(
            &command,
            &std::collections::HashMap::new(),
            &state,
        ),
        None => crabster_core::admin::AdminResponse::xml(
            404,
            "<icestats><error>unknown command</error></icestats>".into(),
        ),
    };

    let _ = writer
        .write_all(
            format!(
                "HTTP/1.0 {} {}\r\nContent-Type: {}\r\nContent-Length: {}\r\n\r\n{}",
                response.status,
                if response.status == 200 {
                    "OK"
                } else {
                    "Error"
                },
                response.content_type,
                response.body.len(),
                response.body
            )
            .as_bytes(),
        )
        .await;
}
