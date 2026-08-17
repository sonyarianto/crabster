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

## Fallback Mounts

When a source disconnects, its listeners can be moved to another mount so the stream never goes silent. Configure per-mount in `crabster.toml`:

```toml
[[mounts]]
mount_name = "/live"
fallback_mount = "/backup"          # where listeners go when /live drops
fallback_when_full = true            # serve fallback when max_listeners is reached
fallback_override = true             # move listeners back when /live reconnects
max_listeners = 100                  # limit used by fallback_when_full
```

Behavior (mirrors Icecast):
- A listener on `/live` is moved to `/backup` when the `/live` source disconnects, and waits (up to 15s) for the fallback source to connect.
- If `/live` is down at connect time, `GET /live` is served from the fallback chain automatically.
- With `fallback_when_full`, listeners beyond `max_listeners` are served the fallback instead of being rejected.
- With `fallback_override`, listeners are moved back to `/live` when its source reconnects.

The fallback chain can be deeper than one hop (up to 10 mounts, like Icecast).

## YP Directory Publishing

Public mounts can be listed in a Yellow Pages directory (e.g. [dir.xiph.org](http://dir.xiph.org)) so listeners can discover them. Enable it in `crabster.toml`:

```toml
yp_url = "http://dir.xiph.org/cgi-bin/yp-cgi"
hostname = "radio.example.org"
```

`hostname` must resolve to this server and be reachable from the public internet, since the directory checks the advertised listen URL. The server sends an `add` when a public mount connects, periodic `touch` updates with the listener count and current song, and a `remove` when it disconnects.

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
