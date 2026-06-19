use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use casteria_core::source::{RingBuffer, Source, SourceInfo, StreamMetadata};
use parking_lot::RwLock as PLRwLock;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tracing::{info, warn};

use crate::ClusterConfig;

pub struct EdgeNode;

impl EdgeNode {
    pub fn new() -> Self {
        Self
    }

    pub async fn start(&self, config: &ClusterConfig, core: casteria_core::SharedState) {
        let origin_host = config.origin_host.as_deref().unwrap_or("127.0.0.1");
        let origin_port = config.origin_port;
        let relays = &config.relays;

        if relays.is_empty() {
            warn!("Edge mode enabled but no relays configured");
            return;
        }

        let origin_host = origin_host.to_string();
        let origin_port = origin_port;
        for relay in relays {
            let mount = relay.local_mount.clone();
            let core = core.clone();
            let origin_host = origin_host.clone();

            tokio::spawn(async move {
                loop {
                    info!("Edge connecting to origin mount {}", mount);

                    match TcpStream::connect((origin_host.clone(), origin_port)).await {
                        Ok(mut stream) => {
                            let request = format!(
                                "GET {} HTTP/1.0\r\n\
                                 Host: {}:{}\r\n\
                                 icy-relay: 1\r\n\
                                 User-Agent: Casteria-Edge/0.1.0\r\n\
                                 \r\n",
                                mount, origin_host, origin_port
                            );

                            if let Err(e) = stream.write_all(request.as_bytes()).await {
                                warn!("Edge write error to origin: {}", e);
                                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                                continue;
                            }

                            let (reader, _writer) = stream.split();
                            let mut buf_reader = BufReader::new(reader);
                            let mut response_line = String::new();

                            if buf_reader.read_line(&mut response_line).await.is_err() {
                                warn!("Edge failed to read response from origin");
                                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                                continue;
                            }

                            if !response_line.contains("200") {
                                warn!(
                                    "Origin returned non-200 for {}: {}",
                                    mount,
                                    response_line.trim()
                                );
                                tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
                                continue;
                            }

                            loop {
                                let mut header_line = String::new();
                                if buf_reader.read_line(&mut header_line).await.is_err() {
                                    break;
                                }
                                if header_line.trim().is_empty() {
                                    break;
                                }
                            }

                            info!("Edge connected to origin for mount {}", mount);

                            let buffer = Arc::new(PLRwLock::new(RingBuffer::new(65536)));
                            let metadata = Arc::new(PLRwLock::new(StreamMetadata::default()));
                            let mount_stats = core.stats.ensure_mount(&mount);

                            let info = Arc::new(SourceInfo {
                                id: uuid::Uuid::new_v4(),
                                mount: mount.clone(),
                                connected_at: std::time::Instant::now(),
                                client_ip: format!("{}:{}", origin_host, origin_port),
                                user_agent: "Casteria-Edge/0.1.0".into(),
                                format: casteria_core::format::FormatType::from_content_type(
                                    "application/octet-stream",
                                ),
                                audio_info: Default::default(),
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
                                metadata: Arc::clone(&metadata),
                                stats: mount_stats,
                            });

                            let source = Arc::new(Source {
                                info: Arc::clone(&info),
                                buffer: Arc::clone(&buffer),
                                connected: AtomicBool::new(true),
                                running: AtomicBool::new(true),
                            });

                            if !core.sources.register(mount.clone(), source) {
                                warn!("Edge mount {} already exists locally", mount);
                                tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
                                continue;
                            }

                            let mut buf = vec![0u8; 16384];
                            loop {
                                match buf_reader.read(&mut buf).await {
                                    Ok(0) => break,
                                    Ok(n) => {
                                        let data = &buf[..n];
                                        buffer.write().write(data);
                                        core.stats
                                            .global()
                                            .total_bytes_received
                                            .fetch_add(n as u64, Ordering::Relaxed);
                                    }
                                    Err(_) => break,
                                }
                            }

                            core.sources.unregister(&mount);
                            info!("Edge disconnected from origin mount {}", mount);
                            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                        }
                        Err(e) => {
                            warn!(
                                "Edge connection failed to {}:{}: {}",
                                origin_host, origin_port, e
                            );
                            tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
                        }
                    }
                }
            });
        }
    }
}
