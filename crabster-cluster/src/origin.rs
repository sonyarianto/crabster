use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;
use tracing::{error, info, warn};

use crate::ClusterConfig;

type EdgeMap = Arc<RwLock<HashMap<String, Vec<broadcast::Sender<Vec<u8>>>>>>;
type WatcherMap = Arc<RwLock<HashMap<String, tokio::task::JoinHandle<()>>>>;

pub struct OriginNode {
    edges: EdgeMap,
    watchers: WatcherMap,
}

impl Default for OriginNode {
    fn default() -> Self {
        Self::new()
    }
}

impl OriginNode {
    pub fn new() -> Self {
        Self {
            edges: Arc::new(RwLock::new(HashMap::new())),
            watchers: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn start(self: Arc<Self>, config: &ClusterConfig, core: crabster_core::SharedState) {
        let port = config.origin_port;
        let addr = format!(
            "{}:{}",
            config.bind_address.as_deref().unwrap_or("0.0.0.0"),
            port
        );

        let listener = match TcpListener::bind(&addr).await {
            Ok(l) => l,
            Err(e) => {
                error!("Failed to bind origin relay on {}: {}", addr, e);
                return;
            }
        };
        info!("Cluster origin listening on {} (relay port)", addr);

        let this = Arc::clone(&self);
        let core_clone = core.clone();
        tokio::spawn(async move {
            this.watch_sources(core_clone).await;
        });

        loop {
            match listener.accept().await {
                Ok((stream, peer)) => {
                    let this = Arc::clone(&self);
                    let core = core.clone();
                    tokio::spawn(async move {
                        if let Err(e) = this.handle_edge(stream, core).await {
                            warn!("Edge connection {} error: {}", peer, e);
                        }
                    });
                }
                Err(e) => error!("Accept error on origin: {}", e),
            }
        }
    }

    async fn watch_sources(&self, core: crabster_core::SharedState) {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(1));
        loop {
            interval.tick().await;
            let mounts = core.sources.all_mounts();
            let mut watchers = self.watchers.write();
            for mount in &mounts {
                if !watchers.contains_key(mount) {
                    let mount_for_spawn = mount.clone();
                    let edges = Arc::clone(&self.edges);
                    let core = core.clone();
                    let handle = tokio::spawn(async move {
                        let mut last_pos: u64 = 0;
                        loop {
                            tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
                            let source = match core.sources.get(&mount_for_spawn) {
                                Some(s) => s,
                                None => break,
                            };
                            if !source.connected.load(std::sync::atomic::Ordering::Relaxed) {
                                break;
                            }
                            let current_pos = source.buffer.read().current_position();
                            if current_pos > last_pos {
                                let data = source.buffer.read().read(last_pos);
                                last_pos = current_pos;
                                if !data.is_empty() {
                                    let edges = edges.read();
                                    if let Some(senders) = edges.get(&mount_for_spawn) {
                                        let data_vec = data.to_vec();
                                        for tx in senders {
                                            let _ = tx.send(data_vec.clone());
                                        }
                                    }
                                }
                            }
                        }
                        let mut edges = edges.write();
                        if let Some(senders) = edges.get_mut(&mount_for_spawn) {
                            senders.retain(|s| s.receiver_count() > 0);
                        }
                    });
                    watchers.insert(mount.clone(), handle);
                }
            }
            watchers.retain(|mount, _| mounts.contains(mount));
        }
    }

    async fn handle_edge(
        &self,
        mut stream: TcpStream,
        core: crabster_core::SharedState,
    ) -> anyhow::Result<()> {
        let (reader, mut writer) = stream.split();
        let mut buf_reader = BufReader::new(reader);
        let mut request_line = String::new();
        buf_reader.read_line(&mut request_line).await?;

        let parts: Vec<&str> = request_line.split_whitespace().collect();
        if parts.len() < 2 || parts[0] != "GET" {
            writer
                .write_all(b"HTTP/1.0 400 Bad Request\r\n\r\n")
                .await?;
            return Ok(());
        }

        let mount = parts[1].split('?').next().unwrap_or("/");

        let mut is_relay = false;
        loop {
            let mut line = String::new();
            buf_reader.read_line(&mut line).await?;
            let trimmed = line.trim();
            if trimmed.is_empty() {
                break;
            }
            if trimmed.to_lowercase().starts_with("icy-relay:") && trimmed.contains('1') {
                is_relay = true;
            }
        }

        if !is_relay {
            writer
                .write_all(
                    b"HTTP/1.0 400 Bad Request\r\n\r\nRelay connections require icy-relay: 1\r\n",
                )
                .await?;
            return Ok(());
        }

        if !core.sources.mount_exists(mount) {
            writer
                .write_all(b"HTTP/1.0 404 Not Found\r\n\r\nNo such mount\r\n")
                .await?;
            return Ok(());
        }

        let (tx, mut rx) = broadcast::channel::<Vec<u8>>(256);
        {
            let mut edges = self.edges.write();
            edges.entry(mount.to_string()).or_default().push(tx);
        }
        info!("Edge connected for mount {}", mount);

        let source = core.sources.get(mount).unwrap();
        let burst_pos = source.buffer.read().current_position();
        let burst = if burst_pos > 0 {
            let data = source.buffer.read().read(0);
            data.to_vec()
        } else {
            Vec::new()
        };

        let _ = writer
            .write_all(b"HTTP/1.0 200 OK\r\nContent-Type: application/octet-stream\r\nicy-relay: 1\r\nCache-Control: no-cache\r\n\r\n")
            .await;

        if !burst.is_empty() {
            let _ = writer.write_all(&burst).await;
        }

        while let Ok(data) = rx.recv().await {
            if writer.write_all(&data).await.is_err() {
                break;
            }
        }

        info!("Edge disconnected from mount {}", mount);
        Ok(())
    }
}
