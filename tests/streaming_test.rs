mod common;

use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test]
async fn test_source_to_listener_data_flow() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let server = common::TestServer::start().await;

        let mount = "/test-stream.mp3";
        let audio_data = generate_test_audio(16384);

        // Connect SOURCE encoder
        let source = tokio::net::TcpStream::connect(server.stream_addr())
            .await
            .unwrap();
        let (mut source_r, mut source_w) = tokio::io::split(source);

        let source_headers = format!(
            "SOURCE {} HTTP/1.0\r\n\
             Authorization: Basic c291cmNlOmhhY2ttZQ==\r\n\
             Content-Type: audio/mpeg\r\n\
             User-Agent: test-encoder/1.0\r\n\r\n",
            mount
        );
        source_w.write_all(source_headers.as_bytes()).await.unwrap();

        // Read first line of response
        let mut response_line = [0u8; 1024];
        let n = source_r.read(&mut response_line).await.unwrap();
        let response = String::from_utf8_lossy(&response_line[..n]);
        assert!(
            response.contains("200 OK"),
            "Expected 200 OK, got: {}",
            response
        );

        // Verify mount appears via API
        let mounts_json = server.api_get("/api/v1/mounts").await.unwrap();
        assert!(
            mounts_json.contains(mount.trim_start_matches('/')),
            "Mount should appear in API response: {}",
            mounts_json
        );

        // Connect GET listener BEFORE sending data, so it catches the stream
        let listener = tokio::net::TcpStream::connect(server.stream_addr())
            .await
            .unwrap();
        let (mut listener_r, mut listener_w) = tokio::io::split(listener);

        let get_request = format!("GET {} HTTP/1.0\r\n\r\n", mount);
        listener_w.write_all(get_request.as_bytes()).await.unwrap();
        drop(listener_w);

        // Read response header (until \r\n\r\n)
        let mut header_buf = Vec::new();
        loop {
            let mut b = [0u8; 1];
            let n = listener_r.read(&mut b).await.unwrap();
            if n == 0 {
                break;
            }
            header_buf.push(b[0]);
            if header_buf.ends_with(b"\r\n\r\n") {
                break;
            }
        }
        let header = String::from_utf8_lossy(&header_buf);
        assert!(header.contains("200 OK"), "Listener got: {}", header);

        // Send audio data in chunks WHILE listener is reading
        let data_sender = tokio::spawn(async move {
            for chunk in audio_data.chunks(4096) {
                source_w.write_all(chunk).await.unwrap();
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        });

        // Read stream data
        let mut stream_data = Vec::new();
        let mut read_buf = [0u8; 4096];
        let start = std::time::Instant::now();
        while start.elapsed() < Duration::from_secs(5) {
            match listener_r.read(&mut read_buf).await {
                Ok(0) => break,
                Ok(n) => stream_data.extend_from_slice(&read_buf[..n]),
                Err(_) => break,
            }
            if stream_data.len() >= 4096 {
                break;
            }
        }

        data_sender.await.ok();

        assert!(
            stream_data.len() >= 1024,
            "Listener should receive at least 1KB, got {}",
            stream_data.len()
        );

        server.shutdown().await;
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn test_source_auth_failure() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let server = common::TestServer::start().await;

        let source = tokio::net::TcpStream::connect(server.stream_addr())
            .await
            .unwrap();
        let (mut source_r, mut source_w) = tokio::io::split(source);

        let bad_headers = "SOURCE /test.mp3 HTTP/1.0\r\n\
             Authorization: Basic d3Jvbmc6d3Jvbmc=\r\n\
             Content-Type: audio/mpeg\r\n\r\n";
        source_w.write_all(bad_headers.as_bytes()).await.unwrap();

        let mut response = [0u8; 1024];
        let n = source_r.read(&mut response).await.unwrap();
        let resp_str = String::from_utf8_lossy(&response[..n]);
        assert!(resp_str.contains("401"), "Expected 401, got: {}", resp_str);

        server.shutdown().await;
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn test_mount_duplicate_rejected() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let server = common::TestServer::start().await;
        let mount = "/dup-test.mp3";
        let addr = server.stream_addr();

        async fn connect_source(addr: &str, mount: &str) -> tokio::io::WriteHalf<tokio::net::TcpStream> {
            let s = tokio::net::TcpStream::connect(addr).await.unwrap();
            let (mut r, mut w) = tokio::io::split(s);
            let h = format!(
                "SOURCE {} HTTP/1.0\r\n\
                 Authorization: Basic c291cmNlOmhhY2ttZQ==\r\n\
                 Content-Type: audio/mpeg\r\n\r\n",
                mount
            );
            w.write_all(h.as_bytes()).await.unwrap();
            let mut resp = [0u8; 1024];
            r.read(&mut resp).await.unwrap();
            w
        }

        let _w1 = connect_source(&addr, mount).await;

        // Second connect on same mount should be rejected
        let s2 = tokio::net::TcpStream::connect(&addr).await.unwrap();
        let (mut r2, mut w2) = tokio::io::split(s2);
        let h2 = format!(
            "SOURCE {} HTTP/1.0\r\n\
             Authorization: Basic c291cmNlOmhhY2ttZQ==\r\n\
             Content-Type: audio/mpeg\r\n\r\n",
            mount
        );
        w2.write_all(h2.as_bytes()).await.unwrap();
        let mut resp = [0u8; 1024];
        r2.read(&mut resp).await.unwrap();
        let resp_str = String::from_utf8_lossy(&resp);
        assert!(resp_str.contains("403"), "Expected 403, got: {}", resp_str);

        server.shutdown().await;
    })
    .await
    .unwrap();
}

fn generate_test_audio(size: usize) -> Vec<u8> {
    (0..size).map(|i| (i % 256) as u8).collect()
}
