# Docker

## Quick Start

```bash
docker compose up --build
```

This builds the image and starts Casteria on ports:
- `8000` — stream port (SOURCE/GET)
- `8001` — REST API
- `8002` — cluster relay

## Configuration

Create a `casteria.toml` in the project root to override defaults:

```toml
stream_port = 9000
api_port = 9001
db_path = "/data/casteria.db"
jwt_secret = "your-secret-here"
```

The compose file mounts a named volume at `/data` inside the container, so the database persists across restarts.

## Environment Variables

| Variable | Effect |
|----------|--------|
| `RUST_LOG` | Log level (e.g. `casteria=debug`, `casteria=info`) |

## Manual Build

```bash
docker build -t casteria .
docker run -d --name casteria -p 8000:8000 -p 8001:8001 casteria
```
