mod common;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crabster::ServerConfig;
use parking_lot::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

struct MockYp {
    addr: String,
    requests: Arc<Mutex<Vec<HashMap<String, String>>>>,
}

impl MockYp {
    fn url(&self) -> String {
        format!("http://{}/cgi-bin/yp-cgi", self.addr)
    }
}

fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => out.push(b' '),
            b'%' if i + 2 < bytes.len() => {
                let hex = std::str::from_utf8(&bytes[i + 1..i + 3]).unwrap_or("");
                if let Ok(b) = u8::from_str_radix(hex, 16) {
                    out.push(b);
                    i += 2;
                } else {
                    out.push(bytes[i]);
                }
            }
            b => out.push(b),
        }
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn decode_form(body: &str) -> HashMap<String, String> {
    let mut map = HashMap::new();
    for pair in body.split('&') {
        let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
        map.insert(percent_decode(k), percent_decode(v));
    }
    map
}

async fn start_mock_yp() -> MockYp {
    let requests = Arc::new(Mutex::new(Vec::new()));
    let requests_clone = Arc::clone(&requests);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();

    tokio::spawn(async move {
        loop {
            let (mut stream, _) = listener.accept().await.unwrap();
            let requests = Arc::clone(&requests_clone);
            tokio::spawn(async move {
                let mut buf = Vec::new();
                let mut tmp = [0u8; 4096];
                // Read the request head.
                loop {
                    match stream.read(&mut tmp).await {
                        Ok(0) => break,
                        Ok(n) => {
                            buf.extend_from_slice(&tmp[..n]);
                            if buf.windows(4).any(|w| w == b"\r\n\r\n") {
                                break;
                            }
                        }
                        Err(_) => return,
                    }
                }
                let head = String::from_utf8_lossy(&buf);
                let header_end = head.find("\r\n\r\n").unwrap_or(head.len());
                let content_length: usize = head
                    .lines()
                    .find_map(|line| {
                        let line = line.trim();
                        if line.to_lowercase().starts_with("content-length:") {
                            line.split_once(':')
                                .and_then(|(_, v)| v.trim().parse().ok())
                        } else {
                            None
                        }
                    })
                    .unwrap_or(0);
                while buf.len() < header_end + 4 + content_length {
                    match stream.read(&mut tmp).await {
                        Ok(0) => break,
                        Ok(n) => buf.extend_from_slice(&tmp[..n]),
                        Err(_) => return,
                    }
                }
                let body = String::from_utf8_lossy(&buf[header_end + 4..]).to_string();
                requests.lock().push(decode_form(&body));

                // Success response with a session id and touch frequency.
                let resp = "HTTP/1.0 200 OK\r\n\
                            YPResponse: 1\r\n\
                            YPMessage: OK\r\n\
                            SID: test-sid\r\n\
                            TouchFreq: 30\r\n\
                            Content-Length: 0\r\n\r\n";
                let _ = stream.write_all(resp.as_bytes()).await;
            });
        }
    });

    MockYp { addr, requests }
}

async fn wait_for_request(
    mock: &MockYp,
    predicate: impl Fn(&HashMap<String, String>) -> bool,
) -> HashMap<String, String> {
    let start = std::time::Instant::now();
    loop {
        let requests = mock.requests.lock();
        if let Some(req) = requests.iter().find(|r| predicate(r)) {
            return req.clone();
        }
        drop(requests);
        assert!(
            start.elapsed() < Duration::from_secs(25),
            "timed out waiting for a matching YP request"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

#[tokio::test]
async fn test_yp_publishing_add_touch_remove() {
    tokio::time::timeout(Duration::from_secs(60), async {
        let mock = start_mock_yp().await;

        let stream_port = portpicker::pick_unused_port().expect("no free port");
        let api_port = portpicker::pick_unused_port().expect("no free port");
        let db_path = std::env::temp_dir().join(format!("crabster-yp-{}.db", stream_port));

        let server = common::TestServer::start_with(ServerConfig {
            stream_port,
            api_port,
            cluster_enabled: false,
            db_path: Some(db_path.to_string_lossy().to_string()),
            jwt_secret: "test-secret".into(),
            shoutcast_compat: true,
            shoutcast_mount: Some("/yp-mount".into()),
            yp_url: Some(mock.url()),
            hostname: "127.0.0.1".into(),
            ..Default::default()
        })
        .await;

        // Connect a Shoutcast source with full metadata so the add request
        // carries a stream name, genre and bitrate.
        let source = tokio::net::TcpStream::connect(server.stream_addr())
            .await
            .unwrap();
        let (mut source_r, mut source_w) = tokio::io::split(source);
        source_w.write_all(b"hackme\r\n").await.unwrap();
        let mut resp = [0u8; 64];
        let n = source_r.read(&mut resp).await.unwrap();
        assert!(
            String::from_utf8_lossy(&resp[..n]).contains("OK2"),
            "Shoutcast handshake failed"
        );
        let icy_headers =
            "icy-name: YP Test Station\r\nicy-genre: Rock\r\nicy-bitrate: 128\r\nicy-pub: 1\r\n\r\n";
        source_w.write_all(icy_headers.as_bytes()).await.unwrap();

        // The directory should receive an add request with the mount details.
        let add = wait_for_request(&mock, |m| m.get("action").map(|a| a == "add") == Some(true))
            .await;
        assert_eq!(add.get("sn").map(String::as_str), Some("YP Test Station"));
        assert_eq!(add.get("genre").map(String::as_str), Some("Rock"));
        assert_eq!(add.get("b").map(String::as_str), Some("128"));
        assert_eq!(add.get("type").map(String::as_str), Some("audio/mpeg"));
        let expected_listenurl = format!("http://127.0.0.1:{}/yp-mount", stream_port);
        assert_eq!(
            add.get("listenurl").map(String::as_str),
            Some(expected_listenurl.as_str())
        );

        // A touch request (sid + listeners) should follow shortly after.
        let touch =
            wait_for_request(&mock, |m| m.get("action").map(|a| a == "touch") == Some(true)).await;
        assert_eq!(
            touch.get("sid").map(String::as_str),
            Some("test-sid"),
            "touch should use the session id from the add response"
        );
        assert!(touch.contains_key("listeners"));

        // Disconnect the source: the entry should be removed with the sid.
        drop(source_w);
        drop(source_r);
        let remove =
            wait_for_request(&mock, |m| m.get("action").map(|a| a == "remove") == Some(true)).await;
        assert_eq!(
            remove.get("sid").map(String::as_str),
            Some("test-sid"),
            "remove should use the session id"
        );

        server.shutdown().await;
    })
    .await
    .unwrap();
}
