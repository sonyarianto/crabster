mod common;

use crabster::ServerConfig;
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

        async fn connect_source(
            addr: &str,
            mount: &str,
        ) -> tokio::io::WriteHalf<tokio::net::TcpStream> {
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

async fn read_until_header_end(reader: &mut tokio::io::ReadHalf<tokio::net::TcpStream>) -> Vec<u8> {
    let mut buf = Vec::new();
    let mut b = [0u8; 1];
    loop {
        let n = reader.read(&mut b).await.unwrap();
        if n == 0 {
            break;
        }
        buf.push(b[0]);
        if buf.ends_with(b"\r\n\r\n") {
            break;
        }
    }
    buf
}

#[tokio::test]
async fn test_shoutcast_source_password_and_ok2() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let stream_port = portpicker::pick_unused_port().expect("no free port");
        let api_port = portpicker::pick_unused_port().expect("no free port");
        let db_path = std::env::temp_dir().join(format!("crabster-shoutcast-{}.db", stream_port));
        let server = common::TestServer::start_with(ServerConfig {
            stream_port,
            api_port,
            cluster_enabled: false,
            db_path: Some(db_path.to_string_lossy().to_string()),
            jwt_secret: "test-secret".into(),
            shoutcast_compat: true,
            shoutcast_mount: Some("/shoutcast-test".into()),
            ..Default::default()
        })
        .await;

        let mount = "/shoutcast-test";

        // 1. Shoutcast v1 handshake: bare password line, expect OK2
        let source = tokio::net::TcpStream::connect(server.stream_addr())
            .await
            .unwrap();
        let (mut source_r, mut source_w) = tokio::io::split(source);
        source_w.write_all(b"hackme\r\n").await.unwrap();

        let mut resp = [0u8; 64];
        let n = source_r.read(&mut resp).await.unwrap();
        let resp_str = String::from_utf8_lossy(&resp[..n]);
        assert!(resp_str.contains("OK2"), "Expected OK2, got: {}", resp_str);

        // 2. Send ICY headers, expect the icy-caps response
        let icy_headers =
            "icy-name: Shoutcast Test Station\r\nicy-genre: Rock\r\nicy-url: http://example.com\r\n\
             icy-bitrate: 128\r\nicy-pub: 1\r\n\r\n";
        source_w.write_all(icy_headers.as_bytes()).await.unwrap();

        let mut caps = [0u8; 64];
        let n = source_r.read(&mut caps).await.unwrap();
        let caps_str = String::from_utf8_lossy(&caps[..n]);
        assert!(
            caps_str.contains("icy-caps"),
            "Expected icy-caps, got: {}",
            caps_str
        );

        // 3. Listener sees the mount with the ICY metadata from the source
        let listener = tokio::net::TcpStream::connect(server.stream_addr())
            .await
            .unwrap();
        let (mut listener_r, mut listener_w) = tokio::io::split(listener);
        let get_request = format!("GET {} HTTP/1.0\r\n\r\n", mount);
        listener_w.write_all(get_request.as_bytes()).await.unwrap();
        drop(listener_w);

        let header_bytes = read_until_header_end(&mut listener_r).await;
        let header = String::from_utf8_lossy(&header_bytes);
        assert!(header.contains("200 OK"), "Listener got: {}", header);
        assert!(
            header.contains("icy-name: Shoutcast Test Station"),
            "Listener should see source ICY name, got: {}",
            header
        );

        // 4. Stream audio through the Shoutcast source. After `icy-metaint`
        //    (8192) audio bytes, insert an in-stream metadata block: the server
        //    must strip it from the audio and update the source metadata.
        let audio_data = generate_test_audio(16384);
        let data_sender = tokio::spawn(async move {
            source_w.write_all(&audio_data[..8192]).await.unwrap();
            tokio::time::sleep(Duration::from_millis(20)).await;
            let mut payload = b"StreamTitle='Test Song';".to_vec();
            payload.resize(32, 0);
            let mut block = vec![2u8]; // 32-byte payload = 2 units of 16
            block.extend_from_slice(&payload);
            source_w.write_all(&block).await.unwrap();
            tokio::time::sleep(Duration::from_millis(20)).await;
            for chunk in audio_data[8192..].chunks(4096) {
                source_w.write_all(chunk).await.unwrap();
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        });

        let mut stream_data = Vec::new();
        let mut read_buf = [0u8; 4096];
        let start = std::time::Instant::now();
        while start.elapsed() < Duration::from_secs(5) {
            match listener_r.read(&mut read_buf).await {
                Ok(0) => break,
                Ok(n) => stream_data.extend_from_slice(&read_buf[..n]),
                Err(_) => break,
            }
            // Wait until enough audio passed the metadata block to query the API.
            if stream_data.len() >= 9000 {
                break;
            }
        }

        // 5. The in-stream block was stripped (audio intact) and the source
        //    metadata was updated with the stream title.
        let mounts_json = server.api_get("/api/v1/mounts").await.unwrap();
        assert!(
            mounts_json.contains("\"title\":\"Test Song\""),
            "Mount metadata should include the in-stream title, got: {}",
            mounts_json
        );

        data_sender.await.ok();

        assert!(
            stream_data.len() >= 1024,
            "Listener should receive at least 1KB from Shoutcast source, got {}",
            stream_data.len()
        );

        server.shutdown().await;
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn test_shoutcast_invalid_password_rejected() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let stream_port = portpicker::pick_unused_port().expect("no free port");
        let api_port = portpicker::pick_unused_port().expect("no free port");
        let db_path =
            std::env::temp_dir().join(format!("crabster-shoutcast-bad-{}.db", stream_port));
        let server = common::TestServer::start_with(ServerConfig {
            stream_port,
            api_port,
            cluster_enabled: false,
            db_path: Some(db_path.to_string_lossy().to_string()),
            jwt_secret: "test-secret".into(),
            shoutcast_compat: true,
            ..Default::default()
        })
        .await;

        let source = tokio::net::TcpStream::connect(server.stream_addr())
            .await
            .unwrap();
        let (mut source_r, mut source_w) = tokio::io::split(source);
        source_w.write_all(b"wrong-password\r\n").await.unwrap();

        let mut resp = [0u8; 128];
        let n = source_r.read(&mut resp).await.unwrap();
        let resp_str = String::from_utf8_lossy(&resp[..n]);
        assert!(
            resp_str.contains("invalid password"),
            "Expected invalid password, got: {}",
            resp_str
        );
        assert!(
            !resp_str.contains("OK2"),
            "Should not get OK2 for a bad password, got: {}",
            resp_str
        );

        server.shutdown().await;
    })
    .await
    .unwrap();
}

fn generate_test_audio(size: usize) -> Vec<u8> {
    (0..size).map(|i| (i % 256) as u8).collect()
}
