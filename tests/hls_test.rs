mod common;

use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test]
async fn test_hls_playlist_after_source() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let server = common::TestServer::start().await;

        let mount = "/hls-test.mp3";

        // Connect a source to create the mount
        let source = tokio::net::TcpStream::connect(server.stream_addr())
            .await
            .unwrap();
        let (mut source_r, mut source_w) = tokio::io::split(source);

        let headers = format!(
            "SOURCE {} HTTP/1.0\r\n\
             Authorization: Basic c291cmNlOmhhY2ttZQ==\r\n\
             Content-Type: audio/mpeg\r\n\r\n",
            mount
        );
        source_w.write_all(headers.as_bytes()).await.unwrap();

        let mut resp = [0u8; 1024];
        source_r.read(&mut resp).await.unwrap();

        // Send audio data for a bit
        let audio = (0..65536).map(|i| (i % 256) as u8).collect::<Vec<_>>();
        source_w.write_all(&audio).await.unwrap();
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Source manager stores as "/hls-test.mp3"
        let hls_start = server
            .api_get("/api/v1/hls/hls-test.mp3/start")
            .await;
        assert!(
            hls_start.unwrap().contains("ok"),
            "HLS start should return ok"
        );

        // Give HLS time to create a segment (segment_duration=10s)
        tokio::time::sleep(Duration::from_secs(12)).await;

        // Fetch playlist
        let playlist = server
            .api_get("/api/v1/hls/hls-test.mp3/playlist.m3u8")
            .await
            .unwrap();
        assert!(playlist.contains("#EXTM3U"), "Should have M3U header: {playlist}");
        assert!(
            playlist.contains(".ts"),
            "Playlist should reference segments: {playlist}"
        );

        // Extract a segment
        for line in playlist.lines() {
            if line.contains(".ts") {
                let seq: String = line.chars().skip_while(|c| !c.is_ascii_digit()).collect();
                let seq = seq.trim_end_matches(".ts");
                let segment = server
                    .api_get(&format!("/api/v1/hls/hls-test.mp3/segment/{seq}"))
                    .await
                    .unwrap();
                assert!(!segment.is_empty(), "Segment should have data");
                break;
            }
        }

        server.shutdown().await;
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn test_hls_nonexistent_mount() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let server = common::TestServer::start().await;
        let result = server
            .api_get("/api/v1/hls/nonexistent/playlist.m3u8")
            .await;
        match result {
            Ok(body) => {
                assert!(
                    body.contains("error") || body.contains("not found"),
                    "Expected error/not found, got: {body}"
                );
            }
            Err(e) => {
                assert!(
                    e.contains("404") || e.contains("not found"),
                    "Expected 404, got: {e}"
                );
            }
        }
        server.shutdown().await;
    })
    .await
    .unwrap();
}
