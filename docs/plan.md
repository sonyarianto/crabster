# Crabster — Streaming Server

Repository: [github.com/sonyarianto/crabster](https://github.com/sonyarianto/crabster)

## Vision

A drop-in replacement written in Rust that supports 100% protocol parity with the standard streaming protocol while adding modern management, multi-tenancy, clustering, analytics, health monitoring, and HLS output.

Existing encoders (liquidsoap, BUTT, Mixxx, RadioBOSS, SAM Broadcaster) connect without modification.

---

## Architecture Overview

```
┌─────────────┐     ┌──────────────────────────────────────────────────┐
│  Encoders   │────▶│              Crabster Server                     │
│ (SOURCE/PUT)│     │                                                  │
└─────────────┘     │  ┌──────────┐  ┌──────────┐  ┌───────────────┐  │
                    │  │ HTTP     │  │ ICY      │  │ Admin/API     │  │
                    │  │ Listener │  │ Listener │  │ Server        │  │
                    │  └──────────┘  └──────────┘  └───────────────┘  │
                    │  ┌──────────┐  ┌──────────┐  ┌───────────────┐  │
                    │  │ Source   │  │ Relay    │  │ Stats/        │  │
                    │  │ Manager  │  │ Manager  │  │ Analytics     │  │
                    │  └──────────┘  └──────────┘  └───────────────┘  │
                    │  ┌──────────┐  ┌──────────┐  ┌───────────────┐  │
                    │  │ Auth     │  │ HLS      │  │ Health        │  │
                    │  │ Stack    │  │ Packager │  │ Monitor       │  │
                    │  └──────────┘  └──────────┘  └───────────────┘  │
                    └──────────────────────────────────────────────────┘
                                      │
                    ┌─────────────────┼─────────────────┐
                    ▼                 ▼                  ▼
             ┌───────────┐    ┌───────────┐     ┌───────────┐
             │ Listeners │    │ HLS       │     │ Edge      │
             │ (HTTP/ICY)│    │ Clients   │     │ Servers   │
             └───────────┘    └───────────┘     └───────────┘
```

### Technology Stack

| Layer | Choice | Rationale |
|-------|--------|-----------|
| Language | Rust | Performance, memory safety, concurrency |
| HTTP framework | axum | Async, tokio-based, rich middleware |
| Async runtime | tokio | Industry standard async runtime |
| TLS | rustls | Pure Rust TLS, no OpenSSL dependency |
| Config | RON + TOML | Modern format, XML import converter for legacy compat |
| Auth | tower middleware + JWT | Extensible auth stack (htpasswd, LDAP, OAuth2, URL) |
| Database | SQLite (single) / PostgreSQL (cluster) | Tiered storage |
| Stats | Prometheus + custom metrics | Built-in + exportable |
| HLS | m3u8-rs + custom segmenter | LL-HLS support |
| Media | symphonia (decoding) + custom (muxing) | Format-agnostic pipeline |

### Directory Layout

```
crabster/
├── Cargo.toml
├── crabster-core/          # Protocol handling, source management, streaming
│   ├── src/
│   │   ├── source/         # Source connection handling (SOURCE, PUT, ICY)
│   │   ├── listener/       # Listener connection handling (HTTP, ICY)
│   │   ├── format/         # Format plugins (OGG, MP3, AAC, Opus, FLAC)
│   │   ├── relay/          # Relay / slave functionality
│   │   ├── auth/           # Authentication stack
│   │   ├── stats/          # Statistics collection
│   │   └── admin/          # Legacy admin XML commands
│   └── Cargo.toml
├── crabster-api/           # REST API
│   ├── src/
│   │   ├── routes/         # API route handlers
│   │   ├── models/         # API data models
│   │   └── middleware/     # Auth, rate limiting, CORS
│   └── Cargo.toml
├── crabster-dashboard/     # Web dashboard (React frontend)
├── crabster-analytics/     # Analytics pipeline
├── crabster-hls/           # HLS packager
├── crabster-health/        # Stream health monitoring
├── crabster-cluster/       # Clustering / origin-edge
└── docs/
    └── plan.md
```

---

## Phase 1: Protocol-Compatible Replacement

**Goal:** Drop-in replacement — existing encoders work without changes.

### Protocol Reference

From streaming server reference implementation:

#### SOURCE Method

```
SOURCE /mountpoint HTTP/1.0
Authorization: Basic <base64("source:password")>
Content-Type: audio/mpeg
User-Agent: streaming-client/1.0

[stream data]
```

#### PUT Method

```
PUT /mountpoint HTTP/1.1
Authorization: Basic <base64("source:password")>
Content-Type: audio/ogg
Content-Length: ...

[stream data]
```

#### Shoutcast Protocol (ICY) — legacy mode

```
Source connects to port+1 (or same port with shoutcast compat)
Sends: <password>\r\n
Server responds: OK2\r\n
Source sends: icy-name: Station Name\r\n
              icy-genre: Genre\r\n
              icy-bitrate: 128\r\n
              \r\n
Stream: MP3 data with ICY metadata every N bytes
```

#### ICY Metadata in MP3 Streams

```
icy-metaint: 8192

Every 8192 bytes:
  byte 0: length (bytes to follow, max 4080 = 255*16)
  bytes 1-4080: metadata pairs "StreamTitle='...';StreamUrl='...';"
                padded with nulls to multiple of 16
```

### Listener HTTP Headers (Response)

```
HTTP/1.0 200 OK
Content-Type: audio/mpeg
icy-br: 128
icy-name: Station Name
icy-genre: Various
icy-pub: 1
icy-url: http://example.com
icy-metaint: 8192
```

### Status Pages (XML/JSON/XSLT)

The server serves status at:
- `/status.xsl` → HTML (XSLT-transformed)
- `/status-json.xsl` → JSON
- `/admin/stats.xml` → Raw XML stats
- `/admin/listmounts` → Mount list

### Implementation Tasks

_Status legend: ✅ implemented · ⚠️ partial · ❌ not implemented (as of commit `7949e11` + static file serving)_

1. ✅ **TCP Listener** — Bind to configurable ports, accept connections
2. ✅ **HTTP Parser** — Parse SOURCE, PUT, GET, HEAD, POST, OPTIONS methods (OPTIONS/DELETE etc. respond 501)
3. ✅ **Source Authentication** — Basic auth for source connections (+ DB mount passwords, quota check)
4. ✅ **Shoutcast/ICY Compatibility** — Password-line auth, OK2 response
5. ✅ **Format Detection** — Content-Type header → format plugin dispatch
   - Ogg (Vorbis, Opus, Theora, FLAC)
   - WebM/EBML (Matroska)
   - MP3/AAC (generic/legacy)
6. ✅ **Ring Buffer** — Per-source circular buffer for burst-on-connect
7. ❌ **Listener Management** — AVL tree of listeners per mount (`ListenerManager` module exists but is not wired into the GET path)
8. ✅ **ICY Metadata** — Parse and insert metadata at `icy-metaint` intervals (source-side parsing for Shoutcast + HTTP opt-in, listener-side insertion for opted-in GET clients)
9. ✅ **Fallback Mount** — Move listeners to alternate mount on source drop (`fallback_mount`, `fallback_when_full`, `fallback_override` in mount config/DB; listener follows the fallback chain, waits for the fallback source to connect, and moves back with `fallback_override`)
10. ✅ **Intro File** — Send intro file to new listeners before the live stream (`intro` in mount config; missing/unreadable file is logged and skipped)
11. ✅ **Static File Serving** — Serve webroot/adminroot files with MIME type detection and path-traversal protection (`fserve` module; configured via `webroot`/`adminroot` or core config paths)
12. ✅ **Admin Interface (Legacy)** — `/admin/` XML commands:
    - `mountlist`, `listclients`, `kickclient`, `moveclients`
    - `updatemetadata`, `metadata` (for Shoutcast metadata updates)
    - Responses are placeholder XML; not yet wired to live state
13. ✅ **Stats System** — XML/JSON stats reporting (real stats via REST API; legacy XML pages still placeholder)
14. ✅ **Authentication Stack**:
    - ✅ Anonymous (allow all)
    - ✅ Htpasswd (file-based) — reads Apache htpasswd files (`$apr1$`, bcrypt, `{SHA}`, crypt), reloads on file change; unknown user defers to next provider, wrong password rejects
    - ✅ URL-based (HTTP callback)
    - ✅ Static (config-defined credentials)
15. ❌ **XSLT Transform** — XSLT rendering for status pages (`/status.xsl` serves plain HTML)
16. ✅ **YP Directory** — Publish to `dir.xiph.org` (add/touch/remove via `yp_url` + `hostname` config; sends `sn`, `genre`, `type`, `b`, `listenurl` and uses the returned `SID`)

### Config Migration

Accept XML config directly or convert to native config:

```ron
// crabster.ron — native format
CrabsterConfig(
    hostname: "localhost",
    listen_sockets: [
        ListenSocket(
            port: 8000,
            bind_address: None,
            tls: Auto,
        ),
    ],
    authentication: Authentication(
        source_password: "hackme",
        admin_user: "admin",
        admin_password: "hackme",
    ),
    limits: Limits(
        clients: 100,
        sources: 2,
        queue_size: 524288,
        client_timeout: 30,
        header_timeout: 15,
        source_timeout: 10,
        burst_size: 65535,
    ),
    logging: Logging(
        loglevel: Information,
        accesslog: "access.log",
        errorlog: "error.log",
    ),
    mounts: [],
)
```

---

## Phase 2: Modern Management

### REST API

Built with axum on a separate admin port or same port with `/api/v1/` prefix.

#### Endpoints

```
GET    /api/v1/status                           # Server health
GET    /api/v1/stats                            # Global statistics
GET    /api/v1/mounts                           # List mountpoints
GET    /api/v1/mounts/{mount}                   # Mount details
POST   /api/v1/mounts                           # Create mount config
PATCH  /api/v1/mounts/{mount}                   # Update mount config
DELETE /api/v1/mounts/{mount}                   # Remove mount
GET    /api/v1/mounts/{mount}/listeners         # List listeners
GET    /api/v1/mounts/{mount}/listeners/{id}    # Listener details
DELETE /api/v1/mounts/{mount}/listeners/{id}    # Kick listener
POST   /api/v1/mounts/{mount}/move-clients      # Move listeners
GET    /api/v1/sources                          # Active sources
GET    /api/v1/sources/{mount}                  # Source details
GET    /api/v1/listeners                        # All listeners
GET    /api/v1/logs                             # Log access
GET    /api/v1/logs/{logtype}                   # Specific log
GET    /api/v1/users                            # User management
POST   /api/v1/users                            # Create user
PUT    /api/v1/users/{id}                       # Update user
DELETE /api/v1/users/{id}                       # Delete user
GET    /api/v1/analytics/concurrent             # Concurrent listeners
GET    /api/v1/analytics/peak                   # Peak listeners
GET    /api/v1/analytics/duration               # Listening duration
GET    /api/v1/analytics/geo                    # Geo distribution
GET    /api/v1/analytics/devices                # Device breakdown
GET    /api/v1/analytics/referrers              # Referrer stats
PUT    /api/v1/config                           # Update running config
GET    /api/v1/config                           # Get running config
POST   /api/v1/config/reload                    # Reload config from file
GET    /api/v1/health                           # Health check status
GET    /api/v1/health/alerts                    # Active alerts
```

#### Authentication

```
Authorization: Bearer <jwt-token>
```

JWT tokens issued via `/api/v1/auth/login` with admin credentials.

### Web Dashboard

React-based dashboard with:

- Mountpoint overview (active/inactive, listener count, bitrate, format)
- Source status (connected time, bytes sent, metadata)
- Listener browser (IP, user-agent, listening time, geo)
- Real-time analytics (concurrent listeners chart)
- Log viewer with filtering
- User management UI
- Configuration editor

---

## Phase 3: Multi-Tenant

### Tenant Model

```
┌───────────────────────────────────────┐
│  Platform Admin                       │
│  ┌─────────────────────────────────┐  │
│  │ Account: Radio Alpha            │  │
│  │  ├── Station: Rock 101          │  │
│  │  │    ├── Mount: /rock          │  │
│  │  │    ├── Mount: /rock-hd       │  │
│  │  │    └── Listeners: 1500       │  │
│  │  └── Station: Jazz 98           │  │
│  │       ├── Mount: /jazz          │  │
│  │       └── Listeners: 800        │  │
│  ├─────────────────────────────────┤  │
│  │ Account: Podcast Network        │  │
│  │  ├── Station: Tech Talk         │  │
│  │  └── Station: History Hour      │  │
│  └─────────────────────────────────┘  │
└───────────────────────────────────────┘
```

### Database Schema (SQLite/PostgreSQL)

```sql
CREATE TABLE accounts (
    id          TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    email       TEXT,
    plan        TEXT NOT NULL,    -- free, pro, enterprise
    created_at  TEXT NOT NULL,
    max_sources INTEGER DEFAULT 5,
    max_bitrate INTEGER DEFAULT 320,
    max_listeners INTEGER DEFAULT 100
);

CREATE TABLE users (
    id          TEXT PRIMARY KEY,
    account_id  TEXT NOT NULL REFERENCES accounts(id),
    username    TEXT NOT NULL UNIQUE,
    password    TEXT NOT NULL,    -- bcrypt hash
    role        TEXT NOT NULL,    -- admin, operator, listener
    created_at  TEXT NOT NULL
);

CREATE TABLE stations (
    id          TEXT PRIMARY KEY,
    account_id  TEXT NOT NULL REFERENCES accounts(id),
    name        TEXT NOT NULL,
    description TEXT,
    genre       TEXT,
    website     TEXT,
    created_at  TEXT NOT NULL
);

CREATE TABLE mounts (
    id          TEXT PRIMARY KEY,
    station_id  TEXT NOT NULL REFERENCES stations(id),
    mount_name  TEXT NOT NULL,
    source_password TEXT NOT NULL,
    max_listeners   INTEGER DEFAULT -1,
    bitrate     INTEGER,
    format      TEXT,
    public      BOOLEAN DEFAULT TRUE,
    hidden      BOOLEAN DEFAULT FALSE,
    fallback_mount  TEXT,
    fallback_when_full BOOLEAN DEFAULT FALSE,
    UNIQUE(station_id, mount_name)
);
```

### Quota Enforcement

- Per-account source limits
- Per-account listener limits
- Per-account bandwidth limits
- Per-mount listener limits
- Bitrate caps by plan tier

---

## Phase 4: Clustering

### Origin/Edge Architecture

```
                    ┌──────────┐
                    │  Origin  │ ←── Source (encoder)
                    │  Server  │
                    └────┬─────┘
                         │ sync
            ┌────────────┼────────────┐
            ▼            ▼            ▼
       ┌────────┐  ┌────────┐  ┌────────┐
       │ Edge 1 │  │ Edge 2 │  │ Edge 3 │
       │ SG     │  │ TYO    │  │ FRA    │
       └────────┘  └────────┘  └────────┘
            │            │            │
            ▼            ▼            ▼
       Listeners    Listeners    Listeners
```

### Relay Types

1. **Full Relay** — Complete stream copy
2. **On-Demand Relay** — Only connect when listeners exist
3. **Transcoding Relay** — Re-encode to different bitrate/format

### Implementation

- **Origin Server**: Full stream buffer, metadata, listener tracking. Pushes to edges or allows edge pull.
- **Edge Server**: Lightweight, connects to origin on listener demand. Caches stream data. Reports listener stats back to origin.
- **Discovery**: Use `consistent` hashing for mount-to-edge mapping. Optional DNS-based routing.
- **Failover**: If origin goes down, edges can promote to origin. If edge goes down, listeners redirect.

### Key Design Decisions

- **Push-based sync**: Origin pushes to edges via relay connections (reuses existing relay protocol for backward compat)
- **or Pull-based**: Edge connects to origin as a virtual listener, then re-serves to its own listeners
- **Stats aggregation**: Edges report listener counts upstream, origin aggregates for global view

---

## Phase 5: Built-in Analytics

### Metrics Collected

| Metric | Granularity | Storage |
|--------|-------------|---------|
| Concurrent listeners | Per mount, 1s resolution | In-memory ring buffer → Prometheus |
| Peak listeners | Per mount, per hour | SQLite |
| Total connections | Per mount | Prometheus counter |
| Listening duration | Per listener | In-memory → PostgreSQL on disconnect |
| Bandwidth used | Per mount, per listener | Prometheus counter |
| Source bitrate | Per mount | Gauge |
| Metadata changes | Per mount | Log |
| Geo location | Per listener | MaxMind DB → country/city |
| User agent | Per listener | parsed → device type |
| Referrer | Per listener | HTTP Referer header |

### Storage Strategy

- **Real-time**: In-memory time-series ring buffer (last 5 minutes, 1s granularity)
- **Recent**: Prometheus (15-day retention)
- **Historical**: TimescaleDB or PostgreSQL with partitioned tables

### API Endpoints

```
GET /api/v1/analytics/concurrent?mount=/stream&from=...&to=...
  → { data: [{ timestamp: "...", count: 142 }, ...] }

GET /api/v1/analytics/peak?mount=/stream&period=day
  → { peaks: [{ date: "...", peak: 523, at: "..." }, ...] }

GET /api/v1/analytics/geo?mount=/stream
  → { countries: [{ code: "US", count: 450 }, ...] }

GET /api/v1/analytics/devices?mount=/stream
  → { devices: [{ type: "mobile", count: 320 }, ...] }
```

---

## Phase 6: Stream Health Monitoring

### Health Checks

- **Source Connected**: Check if source client is still active
- **Bitrate Drop**: Monitor bitrate against expected, alert if below threshold
- **Metadata Stale**: No metadata update in N minutes
- **Silence Detection**: Audio level analysis (RMS threshold over window)
- **Connection Drops**: Listener disconnect rate spike
- **Queue Backlog**: Stream buffer growing (listeners slower than source)

### Alerts

```rust
enum AlertSeverity { Info, Warning, Critical }
enum AlertTrigger {
    SourceDisconnected,
    BitrateDropped { expected: u32, actual: u32 },
    MetadataStale { minutes: u32 },
    SilenceDetected { duration_secs: u32 },
    ListenerDropSpike { rate: f64 },
    QueueBacklog { bytes: u64 },
}

struct Alert {
    id: Uuid,
    severity: AlertSeverity,
    trigger: AlertTrigger,
    mount: String,
    timestamp: Instant,
    acknowledged: bool,
    resolved: bool,
}
```

### Notification Channels

- Webhook (POST to URL)
- Slack/Discord webhook
- Email (SMTP)
- Custom script execution

### Dashboard

- Health overview panel (green/yellow/red per mount)
- Alert history with filtering
- Silence detection graph (RMS over time)

---

## Phase 7: HLS Support

### Architecture

```
Source ──▶ Crabster ──▶ HLS Packager ──▶ HLS Segments ──▶ Listeners
                 │                       │
                  └── Streaming ──────────┘
```

### HLS Packager

- Reads from stream ring buffer (same as direct listeners)
- Segments audio into TS or fMP4 segments
- Generates and updates `m3u8` playlist
- Supports LL-HLS (Low Latency HLS) for sub-second latency
- Configurable segment duration (2-10 seconds)
- Configurable playlist depth

### HLS Endpoints

```
GET /hls/{mount}/index.m3u8          → Master playlist
GET /hls/{mount}/stream.m3u8         → Media playlist
GET /hls/{mount}/segment-{n}.ts      → MPEG-TS segment
GET /hls/{mount}/segment-{n}.m4s     → fMP4 segment
GET /hls/{mount}/init.m4s            → fMP4 init segment
```

### Format Support

| Input Codec | HLS Output |
|-------------|------------|
| MP3 | MP3-in-TS or AAC-in-fMP4 (transcode) |
| AAC | AAC-in-TS or AAC-in-fMP4 |
| Ogg Vorbis | AAC-in-fMP4 (transcode) |
| Opus | Opus-in-fMP4 (HLS spec) |
| FLAC | FLAC-in-fMP4 |

---

## Reference: Protocol Source Map

Key files in reference implementation for protocol parity:

| File | Function | Crabster Equivalent |
|------|----------|---------------------|
| `connection.c` | Accept loop, request parsing, shoutcast compat | `core::listener::tcp` + `core::http::parser` |
| `source.c` | Source thread, client tree, burst handling | `core::source::manager` |
| `format.c` | Format plugin dispatch | `core::format::registry` |
| `format_ogg.c` | Ogg stream handling | `core::format::ogg` |
| `format_mp3.c` | Generic/MP3 with ICY metadata | `core::format::mp3` |
| `format_ebml.c` | WebM/Matroska | `core::format::ebml` |
| `format_opus.c` | Opus in Ogg | `core::format::opus` |
| `client.c` | Client struct, read/write | `core::client` |
| `admin.c` | Admin XML command dispatch | `core::admin` → `api::routes::legacy` |
| `auth.c` | Authentication stack | `core::auth` |
| `auth_anonymous.c` | Anonymous auth | `core::auth::anonymous` |
| `auth_htpasswd.c` | Htpasswd auth | `core::auth::htpasswd` |
| `auth_url.c` | URL callback auth | `core::auth::url` |
| `auth_static.c` | Static credential auth | `core::auth::static` |
| `stats.c` | Statistics XML/tree | `core::stats` → `analytics::metrics` |
| `slave.c` | Relay/master-slave | `core::relay` → `cluster::edge` |
| `fserve.c` | Static file serving | `core::fileserver` |
| `cfgfile.c` | XML config parsing | `core::config::xml_config` |
| `yp.c` | YP directory publishing | `core::yp` |
| `xslt.c` | XSLT transform | Legacy compat → API replaces |
| `json.c` | JSON rendering | Removed — use serde |
| `geoip.c` | GeoIP lookups | `analytics::geo` |

---

## Phase-by-Phase Implementation Order

```
Phase 1 ─── Core Protocol Parity ───────────────────────────── Epoch 1
    ├── ✅ TCP listener + HTTP parser
    ├── ✅ SOURCE/PUT method handling
    ├── ✅ Shoutcast/ICY compatibility
    ├── ✅ Format detection + plugin dispatch
    ├── ⚠️ Ring buffer (✅) + listener management (❌)
    ├── ✅ ICY metadata (insertion + parsing)
    ├── ✅ Fallback mount + intro file
    ├── ✅ Static file serving
    ├── ✅ Legacy admin interface (placeholder responses)
    ├── ✅ Stats XML/JSON reporting
    ├── ✅ Authentication stack (anonymous, htpasswd, url, static)
    ├── ❌ XSLT rendering
    └── ✅ YP directory publishing

Phase 2 ─── Modern Management ──────────────────────────────── Epoch 2
    ├── REST API (axum)
    ├── JWT authentication
    ├── Web dashboard (React)
    ├── Configuration management API
    └── Real-time stats via WebSocket

Phase 3 ─── Multi-Tenant ───────────────────────────────────── Epoch 3
    ├── Account/station/mount DB schema
    ├── Tenant isolation
    ├── Quota enforcement
    └── Admin UI for tenant management

Phase 4 ─── Clustering ─────────────────────────────────────── Epoch 4
    ├── Origin server mode
    ├── Edge server mode
    ├── Relay management
    ├── Stats aggregation
    └── Automatic failover

Phase 5 ─── Analytics ──────────────────────────────────────── Epoch 5
    ├── Metrics collection pipeline
    ├── Time-series storage
    ├── Analytics API endpoints
    └── Dashboard visualization

Phase 6 ─── Health Monitoring ──────────────────────────────── Epoch 6
    ├── Health check system
    ├── Alert engine
    ├── Notification channels
    └── Health dashboard

Phase 7 ─── HLS Support ────────────────────────────────────── Epoch 7
    ├── HLS packager (TS/fMP4)
    ├── m3u8 playlist generation
    ├── LL-HLS support
    └── Transcoding pipeline
```

---

## Getting Started

```bash
# Initialize project
cargo new crabster
cd crabster

# Add workspace members
mkdir -p crabster-core/src
mkdir -p crabster-api/src
mkdir -p crabster-hls/src
mkdir -p crabster-analytics/src
mkdir -p crabster-health/src
mkdir -p crabster-cluster/src
mkdir -p crabster-dashboard
```

### First Milestone

The first milestone is: **an encoder can connect via SOURCE, and a listener can connect via HTTP and hear the stream.**

This requires:
1. TCP listener on port 8000
2. HTTP request parser (handle SOURCE and GET)
3. Source authentication (hardcoded password initially)
4. Ring buffer (store stream data for burst)
5. Listener response (serve HTTP 200 + stream data)
6. Format-agnostic pass-through (store bytes, serve bytes)
