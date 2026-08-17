# Crabster

[![CI](https://github.com/sonyarianto/crabster/actions/workflows/ci.yml/badge.svg)](https://github.com/sonyarianto/crabster/actions/workflows/ci.yml)

**Repository:** [github.com/sonyarianto/crabster](https://github.com/sonyarianto/crabster)

A streaming media server written in Rust — protocol-compatible with existing encoders and listeners.

Existing clients (liquidsoap, BUTT, Mixxx, RadioBOSS, SAM Broadcaster, VLC, mpv) connect without modification.

## Quick Start

```bash
cargo run
```

```
REST API listening on 0.0.0.0:8001
Streaming server listening on 0.0.0.0:8000
```

## Connect an Encoder

```bash
curl -X SOURCE http://source:hackme@localhost:8000/test.mp3 \
  -H "Content-Type: audio/mpeg" \
  -T somefile.mp3
```

## Shoutcast v1 Sources (legacy)

Enable legacy Shoutcast sources (password line + `OK2` handshake) in `crabster.toml`:

```toml
shoutcast_compat = true
shoutcast_mount = "/live"
```

The source connects to the stream port and sends only the password, then ICY headers. If `shoutcast_mount` is unset, the password is matched against configured mounts, falling back to the global `source_password` on mount `/`.

## Listen

```
http://localhost:8000/test.mp3
```

## API

| Endpoint | Description |
|----------|-------------|
| `GET /api/v1/health` | Server health |
| `GET /api/v1/status` | Full status |
| `GET /api/v1/mounts` | Active mount list |
| `GET /api/v1/sources` | Source connection details |
| `GET /api/v1/stats` | Global stats |

## Configuration

Create `crabster.toml`:

```toml
stream_port = 8000
api_port = 8001
jwt_secret = "change-me"
db_path = "crabster.db"
```

Or use CLI flags:

```bash
cargo run -- --stream-port 8080 --api-port 8081 --db-path /data/crabster.db
```

## Docker

```bash
docker compose up --build
```

See [docs/docker.md](docs/docker.md).

## Default Credentials

| Role   | Username | Password |
|--------|----------|----------|
| Source | `source` | `hackme` |
| Admin  | `admin`  | `admin`   |

## Project

```
crabster-core/        Core protocol, source manager, ring buffer
crabster-api/         REST API server
crabster-db/          SQLite multi-tenant database
crabster-hls/         HLS live packager
crabster-analytics/   Client analytics collector
crabster-health/      Stream health monitoring + alerts
crabster-cluster/     Origin/Edge clustering
```

## Tests

```bash
cargo test --test streaming_test --test api_test --test hls_test \
  -- --test-threads=1
```

License: MIT
