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
    pub shoutcast_compat: bool,
    pub shoutcast_mount: Option<String>,
    pub yp_url: Option<String>,
    pub hostname: String,
    /// Per-mount settings (fallback mount, max listeners, public, etc.).
    #[serde(default)]
    pub mounts: Vec<crabster_core::config::MountConfig>,
    /// Root directory for static web files (defaults to core config webroot).
    pub webroot: Option<String>,
    /// Root directory for static admin files (defaults to core config adminroot).
    pub adminroot: Option<String>,
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
            shoutcast_compat: false,
            shoutcast_mount: None,
            yp_url: None,
            hostname: "localhost".into(),
            mounts: Vec::new(),
            webroot: None,
            adminroot: None,
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

    let mut core_config = Config::default();
    if let Some(listen) = core_config.listen_sockets.first_mut() {
        listen.shoutcast_compat = config.shoutcast_compat;
        listen.shoutcast_mount = config.shoutcast_mount.clone();
    }
    core_config.mounts = config.mounts.clone();
    if let Some(webroot) = config.webroot.clone() {
        core_config.paths.webroot = webroot;
    }
    if let Some(adminroot) = config.adminroot.clone() {
        core_config.paths.adminroot = adminroot;
    }

    let core_state: SharedState = Arc::new(AppState {
        config: RwLock::new(core_config.clone()),
        sources: crabster_core::source::SourceManager::new(),
        listeners: crabster_core::listener::ListenerManager::new(),
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

    // Start YP directory publishing if a directory URL is configured.
    if let Some(yp_url) = config.yp_url.clone() {
        let yp_config = crabster_core::yp::YpConfig {
            url: yp_url,
            hostname: config.hostname.clone(),
            stream_port: config.stream_port,
            ..Default::default()
        };
        let yp_manager = Arc::new(crabster_core::yp::YpManager::new(
            yp_config,
            Arc::clone(&core_state),
        ));
        let _yp_handle = yp_manager.start();
        info!("YP directory publishing enabled");
    }

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
                    let peer_ip = peer_addr.ip().to_string();
                    tokio::spawn(async move {
                        if let Err(e) = handle_connection(stream, peer_ip, state, db).await {
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
    peer_ip: String,
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

    let request_line_trimmed = request_line.trim_end_matches(['\r', '\n']);
    let parts: Vec<&str> = request_line_trimmed.split_whitespace().collect();

    if !(parts.len() >= 2 && is_http_method(parts[0])) {
        // Not an HTTP request line: try the Shoutcast v1 password handshake.
        if shoutcast_compat_enabled(&state).await {
            handle_shoutcast_source(request_line_trimmed, buf_reader, writer, state, db).await;
        }
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
        "GET" | "HEAD" => handle_get(path, &headers_map, writer, state, db, &peer_ip).await,
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
    reader: BufReader<tokio::io::ReadHalf<tokio::net::TcpStream>>,
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
    let settings = resolve_mount_settings(&state, &db, mount).await;

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
        max_listeners: settings.as_ref().and_then(|s| s.max_listeners),
        public: settings.as_ref().map(|s| s.public).unwrap_or(true),
        hidden: settings.as_ref().map(|s| s.hidden).unwrap_or(false),
        fallback_mount: settings.as_ref().and_then(|s| s.fallback_mount.clone()),
        fallback_override: settings
            .as_ref()
            .map(|s| s.fallback_override)
            .unwrap_or(false),
        fallback_when_full: settings
            .as_ref()
            .map(|s| s.fallback_when_full)
            .unwrap_or(false),
        intro: settings.as_ref().and_then(|s| s.intro.clone()),
        burst_size: 65536,
        metadata: Arc::new(parking_lot::RwLock::new(StreamMetadata::default())),
        stats: mount_stats,
    });

    let buffer = Arc::new(parking_lot::RwLock::new(RingBuffer::new(65536)));

    // Advertise and parse in-stream ICY metadata only when the source opts in
    // with the `icy-metadata: 1` request header and the format supports it.
    // Without the opt-in the stream is passed through untouched, so encoders
    // that never send metadata blocks are not corrupted.
    let wants_metadata = headers
        .get("icy-metadata")
        .map(|v| v.trim() == "1")
        .unwrap_or(false);
    let icy_metaint = if wants_metadata {
        state
            .format_registry
            .get(format_type)
            .and_then(|f| f.icy_metadata_interval)
    } else {
        None
    };
    let welcome = match icy_metaint {
        Some(interval) => format!(
            "HTTP/1.0 200 OK\r\nContent-Type: {}\r\nicy-metaint: {}\r\n\r\n",
            content_type, interval
        ),
        None => format!("HTTP/1.0 200 OK\r\nContent-Type: {}\r\n\r\n", content_type),
    };
    run_source_stream(
        mount.to_string(),
        state,
        info,
        buffer,
        SourceStreamOptions {
            welcome: welcome.as_bytes(),
            busy: b"HTTP/1.0 503 Service Unavailable\r\n\r\n",
            icy_metaint,
        },
        reader,
        writer,
    )
    .await;
}

/// Response bytes sent to the source after registration (welcome) and when the
/// mount is already taken (busy), plus the optional SHOUTcast metadata interval
/// used to strip in-stream metadata blocks from the audio.
struct SourceStreamOptions<'a> {
    welcome: &'a [u8],
    busy: &'a [u8],
    icy_metaint: Option<u16>,
}

/// A parsed SHOUTcast metadata payload ("StreamTitle='...';StreamUrl='...';"
/// style).
struct ParsedIcyMetadata {
    title: Option<String>,
    url: Option<String>,
}

/// Parses a "StreamTitle='...';StreamUrl='...';" payload, which may be
/// null-padded to a multiple of 16 bytes.
fn parse_icy_metadata(payload: &str) -> ParsedIcyMetadata {
    let mut parsed = ParsedIcyMetadata {
        title: None,
        url: None,
    };
    for part in payload.split(';') {
        let part = part.trim().trim_matches('\0');
        if let Some((key, value)) = part.split_once('=') {
            let value = value.trim().trim_matches('\'');
            match key.trim() {
                "StreamTitle" => parsed.title = Some(value.to_string()),
                "StreamUrl" => parsed.url = Some(value.to_string()),
                _ => {}
            }
        }
    }
    parsed
}

/// Strips SHOUTcast in-stream metadata blocks (a 1-byte length in 16-byte
/// units followed by the payload, inserted by the source every `metaint`
/// audio bytes) so listeners only receive clean audio.
struct IcyMetaParser {
    metaint: usize,
    remaining_audio: usize,
    meta_len: usize,
    meta_collected: usize,
    meta_buf: Vec<u8>,
}

impl IcyMetaParser {
    fn new(metaint: u16) -> Self {
        let metaint = metaint as usize;
        Self {
            metaint,
            remaining_audio: metaint,
            meta_len: 0,
            meta_collected: 0,
            meta_buf: Vec::new(),
        }
    }

    /// Feed raw source bytes; returns the audio bytes to store, plus the parsed
    /// metadata whenever a block completes.
    fn feed(&mut self, mut data: &[u8]) -> (Vec<u8>, Option<ParsedIcyMetadata>) {
        let mut audio = Vec::with_capacity(data.len());
        let mut parsed = None;
        while !data.is_empty() {
            if self.meta_len == 0 {
                if self.remaining_audio > 0 {
                    let take = self.remaining_audio.min(data.len());
                    audio.extend_from_slice(&data[..take]);
                    self.remaining_audio -= take;
                    data = &data[take..];
                } else {
                    // Next byte is the metadata block length in 16-byte units.
                    let len_byte = data[0];
                    data = &data[1..];
                    if len_byte == 0 {
                        self.remaining_audio = self.metaint;
                    } else {
                        self.meta_len = len_byte as usize * 16;
                        self.meta_collected = 0;
                        self.meta_buf.clear();
                    }
                }
            } else {
                let take = (self.meta_len - self.meta_collected).min(data.len());
                self.meta_buf.extend_from_slice(&data[..take]);
                self.meta_collected += take;
                data = &data[take..];
                if self.meta_collected == self.meta_len {
                    parsed = Some(parse_icy_metadata(&String::from_utf8_lossy(&self.meta_buf)));
                    self.meta_len = 0;
                    self.remaining_audio = self.metaint;
                }
            }
        }
        (audio, parsed)
    }
}

/// Inserts SHOUTcast in-stream metadata blocks into the audio sent to a
/// listener every `metaint` audio bytes. A single 0-length block is sent when
/// the metadata has not changed since the previous block.
struct IcyMetaInserter {
    metaint: usize,
    remaining_audio: usize,
    last_title: Option<String>,
    last_url: Option<String>,
}

impl IcyMetaInserter {
    fn new(metaint: u16) -> Self {
        let metaint = metaint as usize;
        Self {
            metaint,
            remaining_audio: metaint,
            last_title: None,
            last_url: None,
        }
    }

    /// Builds the block to insert: a 1-byte length (in 16-byte units) followed
    /// by the null-padded "StreamTitle='...';StreamUrl='...';" payload, or a
    /// single 0 byte when nothing changed.
    fn block(&mut self, title: Option<&str>, url: Option<&str>) -> Vec<u8> {
        if self.last_title == title.map(str::to_string) && self.last_url == url.map(str::to_string)
        {
            return vec![0];
        }
        self.last_title = title.map(str::to_string);
        self.last_url = url.map(str::to_string);

        let mut payload = String::new();
        if let Some(title) = title {
            payload.push_str(&format!("StreamTitle='{}';", title));
        }
        if let Some(url) = url {
            payload.push_str(&format!("StreamUrl='{}';", url));
        }
        if payload.is_empty() {
            return vec![0];
        }
        let units = payload.len().div_ceil(16);
        let mut block = vec![units as u8];
        block.extend_from_slice(payload.as_bytes());
        block.resize(1 + units * 16, 0);
        block
    }
}

/// Writes audio to a listener, inserting SHOUTcast metadata blocks every
/// `metaint` audio bytes when the listener opted in.
async fn write_with_icy_metadata(
    inserter: &mut Option<IcyMetaInserter>,
    data: &[u8],
    source: &Source,
    writer: &mut tokio::io::WriteHalf<tokio::net::TcpStream>,
) -> std::io::Result<()> {
    let Some(inserter) = inserter else {
        return writer.write_all(data).await;
    };

    let (title, url) = {
        let meta = source.info.metadata.read();
        (meta.title.clone(), meta.url.clone())
    };

    let mut out = Vec::with_capacity(data.len() + 64);
    let mut chunk = data;
    while !chunk.is_empty() {
        if inserter.remaining_audio == 0 {
            out.extend_from_slice(&inserter.block(title.as_deref(), url.as_deref()));
            inserter.remaining_audio = inserter.metaint;
        } else {
            let take = inserter.remaining_audio.min(chunk.len());
            out.extend_from_slice(&chunk[..take]);
            inserter.remaining_audio -= take;
            chunk = &chunk[take..];
        }
    }
    writer.write_all(&out).await
}

/// Registers a source and pumps its bytes into the ring buffer until EOF,
/// then unregisters it. Shared by the HTTP SOURCE/PUT and Shoutcast v1 paths.
async fn run_source_stream(
    mount: String,
    state: SharedState,
    info: Arc<SourceInfo>,
    buffer: Arc<parking_lot::RwLock<RingBuffer>>,
    options: SourceStreamOptions<'_>,
    mut reader: BufReader<tokio::io::ReadHalf<tokio::net::TcpStream>>,
    mut writer: tokio::io::WriteHalf<tokio::net::TcpStream>,
) {
    let source = Arc::new(Source {
        info: Arc::clone(&info),
        buffer: Arc::clone(&buffer),
        connected: std::sync::atomic::AtomicBool::new(true),
        running: std::sync::atomic::AtomicBool::new(true),
    });

    if !state.sources.register(mount.clone(), Arc::clone(&source)) {
        let _ = writer.write_all(options.busy).await;
        return;
    }

    if writer.write_all(options.welcome).await.is_err() {
        source
            .connected
            .store(false, std::sync::atomic::Ordering::Relaxed);
        source
            .running
            .store(false, std::sync::atomic::Ordering::Relaxed);
        state.sources.unregister(&mount);
        return;
    }

    info!("Source connected: {} ({})", mount, info.format.mime_type());

    let mut icy_parser = options.icy_metaint.map(IcyMetaParser::new);
    let mut buf = vec![0u8; 8192];
    loop {
        match reader.read(&mut buf).await {
            Ok(0) => break,
            Ok(n) => {
                let raw = &buf[..n];
                if let Some(parser) = &mut icy_parser {
                    let (data, parsed) = parser.feed(raw);
                    if let Some(metadata) = parsed {
                        let mut meta = info.metadata.write();
                        if let Some(title) = metadata.title {
                            meta.title = Some(title);
                        }
                        if let Some(url) = metadata.url {
                            meta.url = Some(url);
                        }
                        drop(meta);
                        info.stats.write().metadata_updates += 1;
                    }
                    if !data.is_empty() {
                        buffer.write().write(&data);
                        state
                            .stats
                            .global()
                            .total_bytes_received
                            .fetch_add(data.len() as u64, std::sync::atomic::Ordering::Relaxed);
                    }
                } else {
                    buffer.write().write(raw);
                    state
                        .stats
                        .global()
                        .total_bytes_received
                        .fetch_add(n as u64, std::sync::atomic::Ordering::Relaxed);
                }
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
    state.sources.unregister(&mount);
    info!("Source disconnected: {}", mount);
}

fn is_http_method(method: &str) -> bool {
    matches!(
        method.to_uppercase().as_str(),
        "SOURCE"
            | "PUT"
            | "GET"
            | "HEAD"
            | "POST"
            | "OPTIONS"
            | "DELETE"
            | "PATCH"
            | "TRACE"
            | "CONNECT"
    )
}

async fn shoutcast_compat_enabled(state: &SharedState) -> bool {
    state
        .config
        .read()
        .await
        .listen_sockets
        .first()
        .map(|l| l.shoutcast_compat)
        .unwrap_or(false)
}

/// Resolve the mount for a Shoutcast v1 password connection and validate the
/// password against it. Mirrors Icecast: use the configured `shoutcast_mount`
/// if set, otherwise match the password against configured mounts, falling
/// back to the global source password on mount "/".
async fn resolve_shoutcast_mount(
    password: &str,
    state: &SharedState,
    db: &Option<crabster_db::Database>,
) -> Option<String> {
    let configured_mount = {
        let config = state.config.read().await;
        config
            .listen_sockets
            .first()
            .and_then(|l| l.shoutcast_mount.clone())
    };

    if let Some(mount) = configured_mount {
        let password_ok = if let Some(ref db) = db {
            match db.get_mount_config(&mount) {
                Ok(Some(config)) => config.source_password == password,
                Ok(None) => {
                    let config = state.config.read().await;
                    password == config.authentication.source_password
                }
                Err(_) => false,
            }
        } else {
            let config = state.config.read().await;
            password == config.authentication.source_password
        };
        return password_ok.then_some(mount);
    }

    // No explicit shoutcast mount: match the password against configured mounts.
    if let Some(ref db) = db {
        if let Ok(Some(config)) = db.find_mount_by_source_password(password) {
            return Some(config.mount_name);
        }
    }

    let config = state.config.read().await;
    (password == config.authentication.source_password).then_some("/".into())
}

/// Handles a legacy Shoutcast v1 source: a bare password line instead of an
/// HTTP request. On success replies `OK2`, parses the ICY headers that follow,
/// then streams the source into the ring buffer.
async fn handle_shoutcast_source(
    password_line: &str,
    mut reader: BufReader<tokio::io::ReadHalf<tokio::net::TcpStream>>,
    mut writer: tokio::io::WriteHalf<tokio::net::TcpStream>,
    state: SharedState,
    db: Option<crabster_db::Database>,
) {
    let password = password_line.trim();
    if password.is_empty() {
        return;
    }

    let mount = match resolve_shoutcast_mount(password, &state, &db).await {
        Some(m) => m,
        None => {
            let _ = writer.write_all(b"invalid password\r\n").await;
            return;
        }
    };

    if state.sources.mount_exists(&mount) {
        let _ = writer.write_all(b"mountpoint in use\r\n").await;
        return;
    }

    let _ = writer.write_all(b"OK2\r\n").await;

    // Read ICY headers terminated by an empty line.
    let mut icy_headers: std::collections::HashMap<String, String> =
        std::collections::HashMap::new();
    loop {
        let mut line = String::new();
        match reader.read_line(&mut line).await {
            Ok(0) => break,
            Err(_) => return,
            Ok(_) => {}
        }
        let trimmed = line.trim_end_matches(['\r', '\n']);
        if trimmed.is_empty() {
            break;
        }
        if let Some((key, value)) = trimmed.split_once(':') {
            icy_headers.insert(key.trim().to_lowercase(), value.trim().to_string());
        }
    }

    let bitrate = icy_headers
        .get("icy-bitrate")
        .and_then(|b| b.parse::<u32>().ok());
    let public = icy_headers
        .get("icy-pub")
        .map(|p| p.trim() == "1")
        .unwrap_or(true);
    let icy_name = icy_headers.get("icy-name").cloned();
    let icy_genre = icy_headers.get("icy-genre").cloned();
    let icy_url = icy_headers.get("icy-url").cloned();

    // No Content-Type in the Shoutcast protocol: prefer a DB-configured format,
    // otherwise assume MP3 (the dominant Shoutcast codec).
    let format_type = db
        .as_ref()
        .and_then(|db| db.get_mount_config(&mount).ok().flatten())
        .and_then(|config| config.format)
        .map(|ct| crabster_core::format::FormatType::from_content_type(&ct))
        .unwrap_or(crabster_core::format::FormatType::Mp3);

    let mount_stats = state.stats.ensure_mount(&mount);
    let settings = resolve_mount_settings(&state, &db, &mount).await;
    let info = Arc::new(SourceInfo {
        id: uuid::Uuid::new_v4(),
        mount: mount.clone(),
        connected_at: std::time::Instant::now(),
        client_ip: String::new(),
        user_agent: "Shoutcast/1.0".into(),
        format: format_type,
        audio_info: std::collections::HashMap::new(),
        bitrate,
        quality: None,
        channels: None,
        sample_rate: None,
        max_listeners: settings.as_ref().and_then(|s| s.max_listeners),
        public: settings.as_ref().map(|s| s.public).unwrap_or(public),
        hidden: settings.as_ref().map(|s| s.hidden).unwrap_or(false),
        fallback_mount: settings.as_ref().and_then(|s| s.fallback_mount.clone()),
        fallback_override: settings
            .as_ref()
            .map(|s| s.fallback_override)
            .unwrap_or(false),
        fallback_when_full: settings
            .as_ref()
            .map(|s| s.fallback_when_full)
            .unwrap_or(false),
        intro: settings.as_ref().and_then(|s| s.intro.clone()),
        burst_size: 65536,
        metadata: Arc::new(parking_lot::RwLock::new(StreamMetadata {
            icy_name,
            icy_genre,
            icy_url,
            icy_br: bitrate,
            ..Default::default()
        })),
        stats: mount_stats,
    });

    let buffer = Arc::new(parking_lot::RwLock::new(RingBuffer::new(65536)));

    // Advertise the metadata interval so the source inserts in-stream metadata
    // blocks, which the stream pump then strips and parses.
    let icy_metaint = state
        .format_registry
        .get(format_type)
        .and_then(|f| f.icy_metadata_interval);
    let welcome = match icy_metaint {
        Some(interval) => format!("icy-caps: 11\r\nicy-metaint: {}\r\n\r\n", interval),
        None => "icy-caps: 11\r\n\r\n".to_string(),
    };

    run_source_stream(
        mount,
        state,
        info,
        buffer,
        SourceStreamOptions {
            welcome: welcome.as_bytes(),
            busy: b"mountpoint in use\r\n",
            icy_metaint,
        },
        reader,
        writer,
    )
    .await;
}

/// Max number of fallback hops to follow (mirrors Icecast's MAX_FALLBACK_DEPTH).
const MAX_FALLBACK_DEPTH: usize = 10;

/// Built-in XSLT 1.0 stylesheet for `/status.xsl` (Icecast-style status page).
const DEFAULT_STATUS_XSL: &str = r##"<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0">
<xsl:template match="/">
<html>
<head><title>Crabster Status</title></head>
<body>
<h1>Crabster <xsl:value-of select="server_id"/></h1>
<p>Host: <xsl:value-of select="host"/></p>
<p>Sources: <xsl:value-of select="sources"/> &middot; Listeners: <xsl:value-of select="listeners"/></p>
<xsl:choose>
<xsl:when test="source">
<xsl:for-each select="source">
<section>
<h2>Mount <code><xsl:value-of select="@mount"/></code></h2>
<xsl:if test="server_name"><p><strong><xsl:value-of select="server_name"/></strong></p></xsl:if>
<xsl:if test="title"><p>Now playing: <xsl:value-of select="title"/></p></xsl:if>
<xsl:if test="genre"><p>Genre: <xsl:value-of select="genre"/></p></xsl:if>
<p>Listeners: <xsl:value-of select="listeners"/> (peak <xsl:value-of select="listener_peak"/>) &middot; Bitrate: <xsl:value-of select="bitrate"/></p>
</section>
</xsl:for-each>
</xsl:when>
<xsl:otherwise>
<p>No active mountpoints.</p>
</xsl:otherwise>
</xsl:choose>
</body>
</html>
</xsl:template>
</xsl:stylesheet>
"##;

/// Seconds a listener waits for a fallback source to connect before being
/// dropped (mirrors Icecast's 15s relay/failover hold).
const FALLBACK_WAIT_SECONDS: u64 = 15;

/// Mount settings relevant to fallback handling, merged from the DB mount
/// config when present, otherwise from the file config mounts list.
#[derive(Debug, Clone, Default)]
struct MountFallbackSettings {
    fallback_mount: Option<String>,
    fallback_when_full: bool,
    fallback_override: bool,
    max_listeners: Option<usize>,
    public: bool,
    hidden: bool,
    intro: Option<String>,
}

async fn resolve_mount_settings(
    state: &SharedState,
    db: &Option<crabster_db::Database>,
    mount: &str,
) -> Option<MountFallbackSettings> {
    if let Some(db) = db {
        if let Ok(Some(cfg)) = db.get_mount_config(mount) {
            return Some(MountFallbackSettings {
                fallback_mount: cfg.fallback_mount,
                fallback_when_full: cfg.fallback_when_full,
                fallback_override: cfg.fallback_override,
                max_listeners: cfg.max_listeners.map(|v| v as usize),
                public: cfg.public,
                hidden: cfg.hidden,
                // DB mount configs have no intro field; file config only.
                intro: None,
            });
        }
    }

    let config = state.config.read().await;
    for m in &config.mounts {
        if m.mount_name == mount {
            return Some(MountFallbackSettings {
                fallback_mount: m.fallback_mount.clone(),
                fallback_when_full: m.fallback_when_full.unwrap_or(false),
                fallback_override: m.fallback_override.unwrap_or(false),
                max_listeners: m.max_listeners.map(|v| v as usize),
                public: m.public.unwrap_or(true),
                hidden: m.hidden.unwrap_or(false),
                intro: m.intro.clone(),
            });
        }
    }
    None
}

/// Resolves the source to serve for a mount request, following the fallback
/// chain when the requested mount has no active source (or is full with
/// `fallback_when_full`). Returns the source and the mount actually served.
async fn resolve_source_with_fallback(
    state: &SharedState,
    db: &Option<crabster_db::Database>,
    requested_mount: &str,
) -> Option<(Arc<Source>, String)> {
    let mut mount = requested_mount.to_string();
    for _ in 0..MAX_FALLBACK_DEPTH {
        if let Some(source) = state.sources.get(&mount) {
            let settings = resolve_mount_settings(state, db, &mount).await;
            let full = settings
                .as_ref()
                .and_then(|s| s.max_listeners)
                .map(|max| source.info.stats.read().current_listeners as usize >= max)
                .unwrap_or(false);
            if !(full
                && settings
                    .as_ref()
                    .map(|s| s.fallback_when_full)
                    .unwrap_or(false))
            {
                return Some((source, mount));
            }
            // Mount is full and fallback_when_full: fall through to fallback.
        }
        let settings = resolve_mount_settings(state, db, &mount).await;
        match settings.and_then(|s| s.fallback_mount) {
            Some(fb) if !fb.is_empty() && fb != mount => mount = fb,
            _ => return None,
        }
    }
    None
}

/// Attempts to move a listener to a better source: back to the requested
/// mount when it has reconnected with `fallback_override`, otherwise to the
/// fallback of the currently served mount. Waits up to `FALLBACK_WAIT_SECONDS`
/// for a fallback source to connect. Returns true if the source changed.
async fn move_listener_to_fallback(
    state: &SharedState,
    db: &Option<crabster_db::Database>,
    requested_mount: &str,
    source: &mut Arc<Source>,
    serving_mount: &mut String,
) -> bool {
    // Serving a fallback: move back to the requested mount when it has
    // reconnected and fallback_override is enabled.
    if *serving_mount != requested_mount {
        let settings = resolve_mount_settings(state, db, requested_mount).await;
        if settings.map(|s| s.fallback_override).unwrap_or(false) {
            if let Some(primary) = state.sources.get(requested_mount) {
                *source = primary;
                *serving_mount = requested_mount.to_string();
                return true;
            }
        }
    }

    // Follow the fallback chain from the currently served mount.
    let mut target = source.info.fallback_mount.clone();
    if target.is_none() {
        let settings = resolve_mount_settings(state, db, serving_mount).await;
        target = settings.and_then(|s| s.fallback_mount);
    }
    let Some(target) = target else { return false };
    if target.is_empty() || target == *serving_mount {
        return false;
    }

    // Wait for the fallback source to connect (listeners are held, like
    // Icecast holds clients during a failover).
    let deadline =
        std::time::Instant::now() + tokio::time::Duration::from_secs(FALLBACK_WAIT_SECONDS);
    loop {
        if let Some(fb_source) = state.sources.get(&target) {
            *source = fb_source;
            *serving_mount = target;
            return true;
        }
        if std::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    }
}

fn listener_joined(state: &SharedState, source: &Arc<Source>) {
    let mut stats = source.info.stats.write();
    stats.current_listeners += 1;
    stats.total_listener_connections += 1;
    stats.last_listener_connect = Some(chrono::Utc::now());
    if stats.current_listeners > stats.peak_listeners {
        stats.peak_listeners = stats.current_listeners;
        stats.peak_listeners_at = Some(chrono::Utc::now());
    }
    drop(stats);

    let global = state.stats.global();
    let cur = global
        .current_listeners
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        + 1;
    let _ = global
        .peak_listeners
        .fetch_max(cur, std::sync::atomic::Ordering::Relaxed);
}

fn listener_left(state: &SharedState, source: &Arc<Source>) {
    let mut stats = source.info.stats.write();
    stats.current_listeners = stats.current_listeners.saturating_sub(1);
    stats.last_listener_disconnect = Some(chrono::Utc::now());
    drop(stats);

    state
        .stats
        .global()
        .current_listeners
        .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
}

async fn handle_get(
    path: &str,
    headers: &std::collections::HashMap<String, String>,
    mut writer: tokio::io::WriteHalf<tokio::net::TcpStream>,
    state: SharedState,
    db: Option<crabster_db::Database>,
    peer_ip: &str,
) {
    let path = path.split('?').next().unwrap_or(path);

    if path == "/" || path == "/status.xsl" || path == "/status-json.xsl" {
        let xml = crabster_core::stats::xml::stats_xml(&state).await;
        let body = if path == "/status-json.xsl" {
            crabster_core::stats::xml::stats_json(&state).await
        } else {
            // Transform the live stats XML with the built-in status stylesheet
            // (an XSLT 1.0 subset, mirroring Icecast's /status.xsl).
            match crabster_core::xslt::transform(&xml, DEFAULT_STATUS_XSL) {
                Ok(html) => html,
                Err(e) => {
                    warn!("XSLT transform failed: {}", e);
                    format!(
                        "<html><body><h1>Crabster</h1><pre>{}</pre></body></html>",
                        xml
                    )
                }
            }
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
        // Try a static file from adminroot first (e.g. admin UI assets),
        // falling back to the legacy XML admin commands.
        let adminroot = state.config.read().await.paths.adminroot.clone();
        if crabster_core::fserve::try_serve(&adminroot, path, &mut writer)
            .await
            .unwrap_or(false)
        {
            return;
        }
        handle_legacy_admin(path, writer, state).await;
        return;
    }

    let requested_mount = path.to_string();
    let (mut source, mut serving_mount) = match resolve_source_with_fallback(
        &state,
        &db,
        &requested_mount,
    )
    .await
    {
        Some((s, m)) => (s, m),
        None => {
            // No active mount: try a static file from webroot before 404.
            let webroot = state.config.read().await.paths.webroot.clone();
            if crabster_core::fserve::try_serve(&webroot, path, &mut writer)
                .await
                .unwrap_or(false)
            {
                return;
            }
            let _ = writer
                .write_all(b"HTTP/1.0 404 Not Found\r\nContent-Type: text/plain\r\n\r\nNo such mountpoint\r\n")
                .await;
            return;
        }
    };

    let format_info = state.format_registry.get(source.info.format).unwrap();

    let (icy_br, icy_name, icy_genre, icy_url, icy_pub) = {
        let meta = source.info.metadata.read();
        (
            meta.icy_br.unwrap_or(128),
            meta.icy_name
                .clone()
                .unwrap_or_else(|| "Crabster Stream".into()),
            meta.icy_genre.clone().unwrap_or_else(|| "Various".into()),
            meta.icy_url.clone().unwrap_or_default(),
            if source.info.public { "1" } else { "0" },
        )
    };

    // Only advertise and insert in-stream ICY metadata for listeners that opt
    // in via the icy-metadata request header; everyone else gets a clean stream.
    let wants_metadata = headers
        .get("icy-metadata")
        .map(|v| v.trim() == "1")
        .unwrap_or(false);
    let icy_metaint = if wants_metadata {
        format_info.icy_metadata_interval
    } else {
        None
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

    if let Some(interval) = icy_metaint {
        response.push_str(&format!("icy-metaint: {}\r\n", interval));
    }

    response.push_str(
        "Access-Control-Allow-Origin: *\r\n\
         Cache-Control: no-cache\r\n\
         Pragma: no-cache\r\n\
         Expires: 0\r\n\r\n",
    );

    if writer.write_all(response.as_bytes()).await.is_err() {
        return;
    }

    // Send the intro file (if configured for the served mount) before the
    // live stream, mirroring Icecast's per-listener intro playback. A missing
    // or unreadable intro file is logged and skipped.
    if let Some(intro_path) = source.info.intro.clone() {
        match tokio::fs::read(&intro_path).await {
            Ok(intro_bytes) => {
                if writer.write_all(&intro_bytes).await.is_err() {
                    return;
                }
            }
            Err(e) => {
                warn!(
                    "Failed to read intro file {} for {}: {}",
                    intro_path, serving_mount, e
                );
            }
        }
    }

    listener_joined(&state, &source);

    // Register this listener with the ListenerManager so it can be queried
    // and kicked via the API / admin interface.
    let (event_tx, _event_rx) = tokio::sync::mpsc::unbounded_channel();
    let listener = Arc::new(crabster_core::listener::Listener {
        info: Arc::new(parking_lot::RwLock::new(
            crabster_core::listener::ListenerInfo {
                id: uuid::Uuid::new_v4(),
                mount: serving_mount.clone(),
                ip: peer_ip.to_string(),
                user_agent: headers.get("user-agent").cloned().unwrap_or_default(),
                protocol: crabster_core::listener::ListenerProtocol::Http,
                connected_at: std::time::Instant::now(),
                bytes_sent: 0,
                referer: headers.get("referer").cloned(),
                country: None,
            },
        )),
        sender: event_tx,
        disconnected: std::sync::atomic::AtomicBool::new(false),
    });
    let listener_id = listener.info.read().id;
    state
        .listeners
        .add_listener(serving_mount.clone(), Arc::clone(&listener));

    let mut buf_clone = Arc::clone(&source.buffer);
    let mut pos = buf_clone.read().current_position();
    let mut meta_inserter = icy_metaint.map(IcyMetaInserter::new);

    loop {
        // The listener was kicked via the API / admin interface.
        if listener
            .disconnected
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            break;
        }

        // Drain any buffered audio first so bytes written just before the
        // source disconnects are not left behind.
        let current_pos = buf_clone.read().current_position();
        if current_pos > pos {
            let data = buf_clone.read().read(pos);
            pos = current_pos;
            if !data.is_empty() {
                listener.info.write().bytes_sent += data.len() as u64;
                if write_with_icy_metadata(&mut meta_inserter, &data, &source, &mut writer)
                    .await
                    .is_err()
                {
                    break;
                }
            }
        } else if !source.connected.load(std::sync::atomic::Ordering::Relaxed) {
            // The source is gone: try to move to the fallback mount (or back
            // to the requested mount with fallback_override), then keep
            // streaming. When no fallback is configured or it never connects,
            // the listener is dropped.
            let old_source = Arc::clone(&source);
            let old_mount = serving_mount.clone();
            let moved = move_listener_to_fallback(
                &state,
                &db,
                &requested_mount,
                &mut source,
                &mut serving_mount,
            )
            .await;
            if !moved {
                break;
            }
            listener_left(&state, &old_source);
            state.listeners.remove_listener(&old_mount, listener_id);
            listener.info.write().mount = serving_mount.clone();
            state
                .listeners
                .add_listener(serving_mount.clone(), Arc::clone(&listener));
            buf_clone = Arc::clone(&source.buffer);
            pos = buf_clone.read().current_position();
            meta_inserter = icy_metaint.map(IcyMetaInserter::new);
            listener_joined(&state, &source);
            continue;
        } else if *serving_mount != requested_mount {
            // The listener is being served by a fallback: move back to the
            // requested mount when its source reconnects and fallback_override
            // is enabled.
            let settings = resolve_mount_settings(&state, &db, &requested_mount).await;
            let override_enabled = settings.map(|s| s.fallback_override).unwrap_or(false);
            if override_enabled {
                if let Some(primary) = state.sources.get(&requested_mount) {
                    listener_left(&state, &source);
                    let old_mount = serving_mount.clone();
                    state.listeners.remove_listener(&old_mount, listener_id);
                    source = primary;
                    serving_mount = requested_mount.clone();
                    listener.info.write().mount = serving_mount.clone();
                    state
                        .listeners
                        .add_listener(serving_mount.clone(), Arc::clone(&listener));
                    buf_clone = Arc::clone(&source.buffer);
                    pos = buf_clone.read().current_position();
                    meta_inserter = icy_metaint.map(IcyMetaInserter::new);
                    listener_joined(&state, &source);
                    continue;
                }
            }
        }

        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    }

    state.listeners.remove_listener(&serving_mount, listener_id);
    listener_left(&state, &source);
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
        Some(command) => {
            crabster_core::admin::handle_admin_command(
                &command,
                &std::collections::HashMap::new(),
                &state,
            )
            .await
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Builds a full SHOUTcast metadata block: 1 length byte (payload size in
    /// 16-byte units) followed by the null-padded payload.
    fn meta_block(title: &str) -> Vec<u8> {
        let payload = format!("StreamTitle='{}';", title);
        let units = payload.len().div_ceil(16);
        let mut block = vec![units as u8];
        let mut body = payload.into_bytes();
        body.resize(units * 16, 0);
        block.extend_from_slice(&body);
        block
    }

    #[test]
    fn icy_parser_passes_audio_through() {
        let mut p = IcyMetaParser::new(8);
        let (audio, parsed) = p.feed(b"12345678");
        assert_eq!(&audio, b"12345678");
        assert!(parsed.is_none());
    }

    #[test]
    fn icy_parser_strips_metadata_block() {
        let mut p = IcyMetaParser::new(8);
        let mut data = b"12345678".to_vec();
        data.extend_from_slice(&meta_block("Test Song"));
        data.extend_from_slice(b"ABCDEFGH");
        let (audio, parsed) = p.feed(&data);
        assert_eq!(&audio, b"12345678ABCDEFGH");
        let parsed = parsed.expect("metadata should be parsed");
        assert_eq!(parsed.title.as_deref(), Some("Test Song"));
    }

    #[test]
    fn icy_parser_handles_zero_length_metadata() {
        let mut p = IcyMetaParser::new(8);
        let mut data = b"12345678".to_vec();
        data.push(0); // empty metadata block
        data.extend_from_slice(b"ABCDEFGH");
        let (audio, parsed) = p.feed(&data);
        assert_eq!(&audio, b"12345678ABCDEFGH");
        assert!(parsed.is_none());
    }

    #[test]
    fn icy_parser_handles_block_straddling_reads() {
        let mut p = IcyMetaParser::new(8);
        let block = meta_block("Straddling Song");
        // 8 audio bytes + the first 2 bytes of the metadata block in read 1
        let mut first = b"12345678".to_vec();
        first.extend_from_slice(&block[..2]);
        let (audio, parsed) = p.feed(&first);
        assert_eq!(&audio, b"12345678");
        assert!(parsed.is_none());

        // rest of the block + more audio in read 2
        let mut second = block[2..].to_vec();
        second.extend_from_slice(b"ABCDEFGH");
        let (audio, parsed) = p.feed(&second);
        assert_eq!(&audio, b"ABCDEFGH");
        assert_eq!(
            parsed.as_ref().and_then(|m| m.title.as_deref()),
            Some("Straddling Song")
        );
    }

    #[test]
    fn icy_parser_handles_multiple_blocks() {
        let mut p = IcyMetaParser::new(8);
        let mut data = b"12345678".to_vec();
        data.extend_from_slice(&meta_block("First Song"));
        data.extend_from_slice(b"ABCDEFGH");
        data.extend_from_slice(&meta_block("Second Song"));
        data.extend_from_slice(b"WXYZ");
        let (audio, parsed) = p.feed(&data);
        assert_eq!(&audio, b"12345678ABCDEFGHWXYZ");
        let parsed = parsed.expect("last metadata block should be returned");
        assert_eq!(parsed.title.as_deref(), Some("Second Song"));
    }

    #[test]
    fn icy_inserter_sends_full_block_then_zero_length() {
        let mut ins = IcyMetaInserter::new(8);
        let first = ins.block(Some("Song One"), None);
        assert_eq!(first[0], 2);
        assert_eq!(first.len(), 33);
        assert!(String::from_utf8_lossy(&first).contains("StreamTitle='Song One';"));

        // unchanged metadata -> 0-length block
        assert_eq!(ins.block(Some("Song One"), None), vec![0]);
    }

    #[test]
    fn icy_inserter_block_changes_with_title() {
        let mut ins = IcyMetaInserter::new(8);
        ins.block(Some("Song One"), None);
        let changed = ins.block(Some("Song Two"), None);
        assert_ne!(changed, vec![0]);
        assert!(String::from_utf8_lossy(&changed).contains("StreamTitle='Song Two';"));
    }

    #[test]
    fn icy_inserter_empty_metadata_is_zero_length() {
        let mut ins = IcyMetaInserter::new(8);
        assert_eq!(ins.block(None, None), vec![0]);
    }
}
