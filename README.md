# Casteria

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

Create `casteria.toml`:

```toml
stream_port = 8000
api_port = 8001
jwt_secret = "change-me"
db_path = "casteria.db"
```

Or use CLI flags:

```bash
cargo run -- --stream-port 8080 --api-port 8081 --db-path /data/casteria.db
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
casteria-core/        Core protocol, source manager, ring buffer
casteria-api/         REST API server
casteria-db/          SQLite multi-tenant database
casteria-hls/         HLS live packager
casteria-analytics/   Client analytics collector
casteria-health/      Stream health monitoring + alerts
casteria-cluster/     Origin/Edge clustering
```

## Tests

```bash
cargo test --test streaming_test --test api_test --test hls_test \
  -- --test-threads=1
```

License: MIT
