FROM rust:1.85-slim-bookworm AS builder

RUN apt-get update && apt-get install -y pkg-config libssl-dev && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY casteria-core/Cargo.toml casteria-core/
COPY casteria-api/Cargo.toml casteria-api/
COPY casteria-hls/Cargo.toml casteria-hls/
COPY casteria-analytics/Cargo.toml casteria-analytics/
COPY casteria-health/Cargo.toml casteria-health/
COPY casteria-cluster/Cargo.toml casteria-cluster/
COPY casteria-db/Cargo.toml casteria-db/

RUN mkdir -p casteria-core/src casteria-api/src casteria-hls/src \
    casteria-analytics/src casteria-health/src casteria-cluster/src casteria-db/src \
    src tests && \
    echo "fn main() {}" > src/main.rs && \
    for d in casteria-core casteria-api casteria-hls casteria-analytics casteria-health casteria-cluster casteria-db; do \
        echo "// placeholder" > $d/src/lib.rs; \
    done

RUN cargo build --release 2>/dev/null || true

COPY src src/
COPY casteria-core/src casteria-core/src/
COPY casteria-api/src casteria-api/src/
COPY casteria-hls/src casteria-hls/src/
COPY casteria-analytics/src casteria-analytics/src/
COPY casteria-health/src casteria-health/src/
COPY casteria-cluster/src casteria-cluster/src/
COPY casteria-db/src casteria-db/src/

RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/casteria /usr/local/bin/casteria

EXPOSE 8000 8001 8002
ENTRYPOINT ["casteria"]
