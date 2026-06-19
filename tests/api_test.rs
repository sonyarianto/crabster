mod common;

use std::time::Duration;

#[tokio::test]
async fn test_api_status() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let server = common::TestServer::start().await;
        let json = server.api_get("/api/v1/status").await.unwrap();
        assert!(json.contains("version"), "Status should have version: {json}");
        server.shutdown().await;
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn test_api_mounts_empty() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let server = common::TestServer::start().await;
        let json = server.api_get("/api/v1/mounts").await.unwrap();
        assert_eq!(json, "[]", "Mounts should be empty array: {json}");
        server.shutdown().await;
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn test_api_stats() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let server = common::TestServer::start().await;
        let json = server.api_get("/api/v1/stats").await.unwrap();
        assert!(json.contains("bytes_received"), "Stats: {json}");
        server.shutdown().await;
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn test_api_health() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let server = common::TestServer::start().await;
        let json = server.api_get("/api/v1/health").await.unwrap();
        assert!(json.contains("status"), "Health should have status: {json}");
        server.shutdown().await;
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn test_api_sources() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let server = common::TestServer::start().await;
        let json = server.api_get("/api/v1/sources").await.unwrap();
        assert_eq!(json, "[]", "Sources should be empty: {json}");
        server.shutdown().await;
    })
    .await
    .unwrap();
}
