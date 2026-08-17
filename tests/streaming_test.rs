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

#[tokio::test]
async fn test_http_source_icy_metadata_opt_in() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let server = common::TestServer::start().await;
        let mount = "/icy-meta.mp3";

        let source = tokio::net::TcpStream::connect(server.stream_addr())
            .await
            .unwrap();
        let (mut source_r, mut source_w) = tokio::io::split(source);

        // Opt in to in-stream metadata via the icy-metadata request header.
        let source_headers = format!(
            "SOURCE {} HTTP/1.0\r\n\
             Authorization: Basic c291cmNlOmhhY2ttZQ==\r\n\
             Content-Type: audio/mpeg\r\n\
             icy-metadata: 1\r\n\r\n",
            mount
        );
        source_w.write_all(source_headers.as_bytes()).await.unwrap();

        let resp_bytes = read_until_header_end(&mut source_r).await;
        let resp = String::from_utf8_lossy(&resp_bytes);
        assert!(resp.contains("200 OK"), "got: {}", resp);
        assert!(
            resp.contains("icy-metaint: 8192"),
            "Opted-in source should get icy-metaint, got: {}",
            resp
        );

        // Stream 8192 audio bytes, then an in-stream metadata block, then more
        // audio, keeping the source connected long enough to query the API.
        let audio_data = generate_test_audio(20000);
        let sender = tokio::spawn(async move {
            source_w.write_all(&audio_data[..8192]).await.unwrap();
            tokio::time::sleep(Duration::from_millis(20)).await;
            let mut payload = b"StreamTitle='HTTP Song';".to_vec();
            payload.resize(32, 0);
            let mut block = vec![2u8]; // 32-byte payload = 2 units of 16
            block.extend_from_slice(&payload);
            source_w.write_all(&block).await.unwrap();
            for chunk in audio_data[8192..].chunks(4096) {
                source_w.write_all(chunk).await.unwrap();
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        });

        let mut title_seen = false;
        for _ in 0..30 {
            let mounts = server.api_get("/api/v1/mounts").await.unwrap();
            if mounts.contains("\"title\":\"HTTP Song\"") {
                title_seen = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(
            title_seen,
            "In-stream title should be parsed from an opted-in HTTP source"
        );

        sender.await.ok();
        server.shutdown().await;
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn test_http_source_without_metadata_opt_in_passthrough() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let server = common::TestServer::start().await;
        let mount = "/plain.mp3";

        let source = tokio::net::TcpStream::connect(server.stream_addr())
            .await
            .unwrap();
        let (mut source_r, mut source_w) = tokio::io::split(source);

        // No icy-metadata header: stream must pass through untouched.
        let source_headers = format!(
            "SOURCE {} HTTP/1.0\r\n\
             Authorization: Basic c291cmNlOmhhY2ttZQ==\r\n\
             Content-Type: audio/mpeg\r\n\r\n",
            mount
        );
        source_w.write_all(source_headers.as_bytes()).await.unwrap();

        let resp_bytes = read_until_header_end(&mut source_r).await;
        let resp = String::from_utf8_lossy(&resp_bytes);
        assert!(resp.contains("200 OK"), "got: {}", resp);
        assert!(
            !resp.contains("icy-metaint"),
            "Source without opt-in should not get icy-metaint, got: {}",
            resp
        );

        // Attach a listener before sending data.
        let listener = tokio::net::TcpStream::connect(server.stream_addr())
            .await
            .unwrap();
        let (mut listener_r, mut listener_w) = tokio::io::split(listener);
        let get_request = format!("GET {} HTTP/1.0\r\n\r\n", mount);
        listener_w.write_all(get_request.as_bytes()).await.unwrap();
        drop(listener_w);
        let _ = read_until_header_end(&mut listener_r).await;

        // Stream more than one metaint worth of audio with no metadata blocks,
        // then disconnect immediately: the listener must still receive every
        // byte (the server drains the buffer on source disconnect).
        let audio_data = generate_test_audio(9000);
        let sender = tokio::spawn(async move {
            for chunk in audio_data.chunks(4096) {
                source_w.write_all(chunk).await.unwrap();
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            // source_w drops here -> EOF -> server marks the source disconnected
        });

        let mut stream_data = Vec::new();
        let mut read_buf = [0u8; 4096];
        let start = std::time::Instant::now();
        while start.elapsed() < Duration::from_secs(5) && stream_data.len() < 9000 {
            match listener_r.read(&mut read_buf).await {
                Ok(0) => break,
                Ok(n) => stream_data.extend_from_slice(&read_buf[..n]),
                Err(_) => break,
            }
        }

        assert_eq!(
            stream_data.len(),
            9000,
            "All audio must pass through even after source disconnect, got {}",
            stream_data.len()
        );

        sender.await.ok();
        server.shutdown().await;
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn test_get_listener_icy_metadata_insertion() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let server = common::TestServer::start().await;
        let mount = "/meta-out.mp3";

        // SOURCE opts in to metadata and streams audio plus one metadata block.
        let source = tokio::net::TcpStream::connect(server.stream_addr())
            .await
            .unwrap();
        let (mut source_r, mut source_w) = tokio::io::split(source);
        let source_headers = format!(
            "SOURCE {} HTTP/1.0\r\n\
             Authorization: Basic c291cmNlOmhhY2ttZQ==\r\n\
             Content-Type: audio/mpeg\r\n\
             icy-metadata: 1\r\n\r\n",
            mount
        );
        source_w.write_all(source_headers.as_bytes()).await.unwrap();
        let _ = read_until_header_end(&mut source_r).await;

        // Listener opts in too: it must get icy-metaint and an inserted block.
        let listener = tokio::net::TcpStream::connect(server.stream_addr())
            .await
            .unwrap();
        let (mut listener_r, mut listener_w) = tokio::io::split(listener);
        let get_request = format!("GET {} HTTP/1.0\r\nicy-metadata: 1\r\n\r\n", mount);
        listener_w.write_all(get_request.as_bytes()).await.unwrap();
        drop(listener_w);

        let header_bytes = read_until_header_end(&mut listener_r).await;
        let header = String::from_utf8_lossy(&header_bytes);
        assert!(header.contains("200 OK"), "got: {}", header);
        assert!(
            header.contains("icy-metaint: 8192"),
            "Listener that opts in should get icy-metaint, got: {}",
            header
        );

        // Stream 8192 audio bytes, then a source metadata block, then more audio.
        let audio_data = generate_test_audio(30000);
        let sender = tokio::spawn(async move {
            source_w.write_all(&audio_data[..8192]).await.unwrap();
            tokio::time::sleep(Duration::from_millis(20)).await;
            let mut payload = b"StreamTitle='Listener Song';".to_vec();
            payload.resize(32, 0);
            let mut block = vec![2u8];
            block.extend_from_slice(&payload);
            source_w.write_all(&block).await.unwrap();
            for chunk in audio_data[8192..].chunks(4096) {
                source_w.write_all(chunk).await.unwrap();
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        });

        // The listener stream is audio with metadata blocks inserted every
        // 8192 audio bytes; the title must appear in a block once the source
        // metadata is parsed (at the latest after the second boundary,
        // ~16.5KB into the stream).
        let mut stream = Vec::new();
        let mut read_buf = [0u8; 4096];
        let start = std::time::Instant::now();
        while stream.len() < 16500 && start.elapsed() < Duration::from_secs(5) {
            match listener_r.read(&mut read_buf).await {
                Ok(0) => break,
                Ok(n) => stream.extend_from_slice(&read_buf[..n]),
                Err(_) => break,
            }
        }
        assert!(
            stream.len() >= 16500,
            "Listener should receive streamed audio plus metadata blocks, got {} bytes",
            stream.len()
        );
        let lossy = String::from_utf8_lossy(&stream);
        assert!(
            lossy.contains("Listener Song"),
            "Inserted metadata block should carry the stream title"
        );

        sender.await.ok();
        server.shutdown().await;
    })
    .await
    .unwrap();
}

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

#[tokio::test]
async fn test_fallback_mount_switches_listener_on_source_disconnect() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let stream_port = portpicker::pick_unused_port().expect("no free port");
        let api_port = portpicker::pick_unused_port().expect("no free port");
        let db_path = std::env::temp_dir().join(format!("crabster-fallback-{}.db", stream_port));
        let server = common::TestServer::start_with(ServerConfig {
            stream_port,
            api_port,
            cluster_enabled: false,
            db_path: Some(db_path.to_string_lossy().to_string()),
            jwt_secret: "test-secret".into(),
            mounts: vec![crabster_core::config::MountConfig {
                mount_name: "/main".into(),
                fallback_mount: Some("/backup".into()),
                ..Default::default()
            }],
            ..Default::default()
        })
        .await;

        let addr = server.stream_addr();
        let mut main_w = connect_source(&addr, "/main").await;
        let mut backup_w = connect_source(&addr, "/backup").await;

        let listener = tokio::net::TcpStream::connect(&addr).await.unwrap();
        let (mut listener_r, mut listener_w) = tokio::io::split(listener);
        listener_w
            .write_all(b"GET /main HTTP/1.0\r\n\r\n")
            .await
            .unwrap();
        drop(listener_w);
        let header = read_until_header_end(&mut listener_r).await;
        assert!(
            String::from_utf8_lossy(&header).contains("200 OK"),
            "Listener should get 200 for /main"
        );

        // Stream distinguishable audio from /main, then disconnect it.
        main_w.write_all(&generate_test_audio(4096)).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        drop(main_w); // source disconnects

        // The listener should be moved to /backup and receive its audio.
        tokio::time::sleep(Duration::from_millis(300)).await;
        backup_w
            .write_all(&generate_test_audio(8192))
            .await
            .unwrap();

        let mut stream_data = Vec::new();
        let mut read_buf = [0u8; 4096];
        let start = std::time::Instant::now();
        while start.elapsed() < Duration::from_secs(5) && stream_data.len() < 8192 {
            match listener_r.read(&mut read_buf).await {
                Ok(0) => break,
                Ok(n) => stream_data.extend_from_slice(&read_buf[..n]),
                Err(_) => break,
            }
        }

        assert!(
            stream_data.len() >= 4096,
            "Listener should receive backup audio after fallback, got {} bytes",
            stream_data.len()
        );

        drop(backup_w);
        server.shutdown().await;
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn test_fallback_mount_serves_when_primary_is_down() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let stream_port = portpicker::pick_unused_port().expect("no free port");
        let api_port = portpicker::pick_unused_port().expect("no free port");
        let db_path =
            std::env::temp_dir().join(format!("crabster-fallback-down-{}.db", stream_port));
        let server = common::TestServer::start_with(ServerConfig {
            stream_port,
            api_port,
            cluster_enabled: false,
            db_path: Some(db_path.to_string_lossy().to_string()),
            jwt_secret: "test-secret".into(),
            mounts: vec![crabster_core::config::MountConfig {
                mount_name: "/main".into(),
                fallback_mount: Some("/backup".into()),
                ..Default::default()
            }],
            ..Default::default()
        })
        .await;

        let addr = server.stream_addr();
        // Only the backup source is up; /main is down.
        let mut backup_w = connect_source(&addr, "/backup").await;

        let listener = tokio::net::TcpStream::connect(&addr).await.unwrap();
        let (mut listener_r, mut listener_w) = tokio::io::split(listener);
        listener_w
            .write_all(b"GET /main HTTP/1.0\r\n\r\n")
            .await
            .unwrap();
        drop(listener_w);
        let header = read_until_header_end(&mut listener_r).await;
        assert!(
            String::from_utf8_lossy(&header).contains("200 OK"),
            "GET /main with primary down should fall back to /backup"
        );

        backup_w
            .write_all(&generate_test_audio(4096))
            .await
            .unwrap();
        let mut stream_data = Vec::new();
        let mut read_buf = [0u8; 4096];
        let start = std::time::Instant::now();
        while start.elapsed() < Duration::from_secs(5) && stream_data.len() < 4096 {
            match listener_r.read(&mut read_buf).await {
                Ok(0) => break,
                Ok(n) => stream_data.extend_from_slice(&read_buf[..n]),
                Err(_) => break,
            }
        }
        assert!(
            stream_data.len() >= 1024,
            "Listener should receive audio from the fallback source, got {} bytes",
            stream_data.len()
        );

        drop(backup_w);
        server.shutdown().await;
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn test_fallback_override_moves_listener_back() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let stream_port = portpicker::pick_unused_port().expect("no free port");
        let api_port = portpicker::pick_unused_port().expect("no free port");
        let db_path =
            std::env::temp_dir().join(format!("crabster-fallback-ovr-{}.db", stream_port));
        let server = common::TestServer::start_with(ServerConfig {
            stream_port,
            api_port,
            cluster_enabled: false,
            db_path: Some(db_path.to_string_lossy().to_string()),
            jwt_secret: "test-secret".into(),
            mounts: vec![crabster_core::config::MountConfig {
                mount_name: "/main".into(),
                fallback_mount: Some("/backup".into()),
                fallback_override: Some(true),
                ..Default::default()
            }],
            ..Default::default()
        })
        .await;

        let addr = server.stream_addr();
        let mut main_w = connect_source(&addr, "/main").await;
        let mut backup_w = connect_source(&addr, "/backup").await;

        // Listener connects to /main.
        let listener = tokio::net::TcpStream::connect(&addr).await.unwrap();
        let (mut listener_r, mut listener_w) = tokio::io::split(listener);
        listener_w
            .write_all(b"GET /main HTTP/1.0\r\n\r\n")
            .await
            .unwrap();
        drop(listener_w);
        let header = read_until_header_end(&mut listener_r).await;
        assert!(String::from_utf8_lossy(&header).contains("200 OK"));

        // /main disconnects -> listener moves to /backup and receives its data.
        drop(main_w);
        tokio::time::sleep(Duration::from_millis(300)).await;
        backup_w
            .write_all(&generate_test_audio(4096))
            .await
            .unwrap();

        let mut stream_data = Vec::new();
        let mut read_buf = [0u8; 4096];
        let start = std::time::Instant::now();
        while start.elapsed() < Duration::from_secs(5) && stream_data.len() < 4096 {
            match listener_r.read(&mut read_buf).await {
                Ok(0) => break,
                Ok(n) => stream_data.extend_from_slice(&read_buf[..n]),
                Err(_) => break,
            }
        }
        assert!(
            stream_data.len() >= 1024,
            "Listener should receive backup audio after /main disconnect, got {} bytes",
            stream_data.len()
        );

        // /main reconnects with fallback_override -> listener moves back and
        // receives the new /main data.
        let mut main_w = connect_source(&addr, "/main").await;
        tokio::time::sleep(Duration::from_millis(300)).await;
        main_w.write_all(&generate_test_audio(8192)).await.unwrap();

        let mut stream_data = Vec::new();
        let mut read_buf = [0u8; 4096];
        let start = std::time::Instant::now();
        while start.elapsed() < Duration::from_secs(5) && stream_data.len() < 8192 {
            match listener_r.read(&mut read_buf).await {
                Ok(0) => break,
                Ok(n) => stream_data.extend_from_slice(&read_buf[..n]),
                Err(_) => break,
            }
        }
        assert!(
            stream_data.len() >= 4096,
            "Listener should move back to /main and receive new data, got {} bytes",
            stream_data.len()
        );

        drop(main_w);
        drop(backup_w);
        server.shutdown().await;
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn test_fallback_when_full_serves_fallback() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let stream_port = portpicker::pick_unused_port().expect("no free port");
        let api_port = portpicker::pick_unused_port().expect("no free port");
        let db_path =
            std::env::temp_dir().join(format!("crabster-fallback-full-{}.db", stream_port));
        let server = common::TestServer::start_with(ServerConfig {
            stream_port,
            api_port,
            cluster_enabled: false,
            db_path: Some(db_path.to_string_lossy().to_string()),
            jwt_secret: "test-secret".into(),
            mounts: vec![crabster_core::config::MountConfig {
                mount_name: "/main".into(),
                max_listeners: Some(1),
                fallback_mount: Some("/backup".into()),
                fallback_when_full: Some(true),
                ..Default::default()
            }],
            ..Default::default()
        })
        .await;

        let addr = server.stream_addr();
        let mut main_w = connect_source(&addr, "/main").await;
        let mut backup_w = connect_source(&addr, "/backup").await;

        // First listener fits on /main (max_listeners = 1).
        let l1 = tokio::net::TcpStream::connect(&addr).await.unwrap();
        let (mut l1_r, mut l1_w) = tokio::io::split(l1);
        l1_w.write_all(b"GET /main HTTP/1.0\r\n\r\n").await.unwrap();
        drop(l1_w);
        let h1 = read_until_header_end(&mut l1_r).await;
        assert!(String::from_utf8_lossy(&h1).contains("200 OK"));

        // Second listener exceeds the limit -> served from /backup.
        let l2 = tokio::net::TcpStream::connect(&addr).await.unwrap();
        let (mut l2_r, mut l2_w) = tokio::io::split(l2);
        l2_w.write_all(b"GET /main HTTP/1.0\r\n\r\n").await.unwrap();
        drop(l2_w);
        let h2 = read_until_header_end(&mut l2_r).await;
        assert!(String::from_utf8_lossy(&h2).contains("200 OK"));

        // Both listeners should receive audio.
        main_w.write_all(&generate_test_audio(4096)).await.unwrap();
        backup_w
            .write_all(&generate_test_audio(4096))
            .await
            .unwrap();

        let d1 = read_audio(&mut l1_r, 4096).await;
        let d2 = read_audio(&mut l2_r, 4096).await;
        assert!(
            d1.len() >= 1024,
            "Listener 1 should receive audio from /main, got {} bytes",
            d1.len()
        );
        assert!(
            d2.len() >= 1024,
            "Listener 2 should receive audio from /backup, got {} bytes",
            d2.len()
        );

        drop(main_w);
        drop(backup_w);
        server.shutdown().await;
    })
    .await
    .unwrap();
}

async fn read_audio(r: &mut tokio::io::ReadHalf<tokio::net::TcpStream>, target: usize) -> Vec<u8> {
    let mut data = Vec::new();
    let mut buf = [0u8; 4096];
    let start = std::time::Instant::now();
    while start.elapsed() < Duration::from_secs(5) && data.len() < target {
        match r.read(&mut buf).await {
            Ok(0) => break,
            Ok(n) => data.extend_from_slice(&buf[..n]),
            Err(_) => break,
        }
    }
    data
}

fn generate_test_audio(size: usize) -> Vec<u8> {
    (0..size).map(|i| (i % 256) as u8).collect()
}
