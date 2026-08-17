#![allow(dead_code)] // not every test binary uses every helper

use std::time::Duration;

use crabster::{run_with_config, ServerConfig};

pub struct TestServer {
    pub stream_port: u16,
    pub api_port: u16,
    shutdown_tx: Option<tokio::sync::oneshot::Sender<()>>,
    handle: Option<tokio::task::JoinHandle<()>>,
}

impl TestServer {
    pub async fn start() -> Self {
        let stream_port = portpicker::pick_unused_port().expect("no free port");
        let api_port = portpicker::pick_unused_port().expect("no free port");

        let db_path = std::env::temp_dir().join(format!("crabster-test-{}.db", stream_port));
        let db_path_str = db_path.to_string_lossy().to_string();

        let config = ServerConfig {
            stream_port,
            api_port,
            cluster_enabled: false,
            db_path: Some(db_path_str),
            jwt_secret: "test-secret".into(),
            ..Default::default()
        };

        Self::start_with(config).await
    }

    pub async fn start_with(config: ServerConfig) -> Self {
        let stream_port = config.stream_port;
        let api_port = config.api_port;

        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

        let handle = tokio::spawn(async move {
            if let Err(e) = run_with_config(config, shutdown_rx).await {
                eprintln!("Test server error: {e}");
            }
        });

        // Wait until both servers are reachable
        let api_addr = format!("127.0.0.1:{}", api_port);
        let listen_addr = format!("127.0.0.1:{}", stream_port);
        for _ in 0..20 {
            let api_ok = tokio::net::TcpStream::connect(&api_addr).await.is_ok();
            let stream_ok = tokio::net::TcpStream::connect(&listen_addr).await.is_ok();
            if api_ok && stream_ok {
                break;
            }
            tokio::time::sleep(Duration::from_millis(300)).await;
        }

        Self {
            stream_port,
            api_port,
            shutdown_tx: Some(shutdown_tx),
            handle: Some(handle),
        }
    }

    pub fn stream_addr(&self) -> String {
        format!("127.0.0.1:{}", self.stream_port)
    }

    pub fn api_url(&self) -> String {
        format!("http://127.0.0.1:{}", self.api_port)
    }

    pub async fn api_get(&self, path: &str) -> Result<String, String> {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let addr = format!("127.0.0.1:{}", self.api_port);
        let mut stream = tokio::net::TcpStream::connect(&addr)
            .await
            .map_err(|e| format!("connect: {e}"))?;

        let request =
            format!("GET {path} HTTP/1.0\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n");
        stream
            .write_all(request.as_bytes())
            .await
            .map_err(|e| format!("write: {e}"))?;

        let mut response = Vec::new();
        let mut buf = [0u8; 4096];
        loop {
            match stream.read(&mut buf).await {
                Ok(0) => break,
                Ok(n) => response.extend_from_slice(&buf[..n]),
                Err(e) => return Err(format!("read: {e}")),
            }
        }
        let response_str = String::from_utf8_lossy(&response).to_string();

        // Extract body after headers (after first \r\n\r\n)
        if let Some(body_start) = response_str.find("\r\n\r\n") {
            Ok(response_str[body_start + 4..].to_string())
        } else {
            Ok(response_str)
        }
    }

    pub async fn shutdown(mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        if let Some(handle) = self.handle.take() {
            let _ = tokio::time::timeout(Duration::from_secs(5), handle).await;
        }
    }
}
