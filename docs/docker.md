# Docker

Source: [github.com/sonyarianto/crabster](https://github.com/sonyarianto/crabster)

## Quick Start

```bash
docker compose up --build
```

This builds the image and starts Crabster on ports:
- `8000` — stream port (SOURCE/GET)
- `8001` — REST API
- `8002` — cluster relay

## Configuration

Create a `crabster.toml` in the project root to override defaults:

```toml
stream_port = 9000
api_port = 9001
db_path = "/data/crabster.db"
jwt_secret = "your-secret-here"
```

The compose file mounts a named volume at `/data` inside the container, so the database persists across restarts.

## Environment Variables

| Variable | Effect |
|----------|--------|
| `RUST_LOG` | Log level (e.g. `crabster=debug`, `crabster=info`) |

## Manual Build

```bash
docker build -t crabster .
docker run -d --name crabster -p 8000:8000 -p 8001:8001 crabster
```
