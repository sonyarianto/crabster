FROM rust:1.85-slim-bookworm AS builder

RUN apt-get update && apt-get install -y pkg-config libssl-dev && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY crabster-core/Cargo.toml crabster-core/
COPY crabster-api/Cargo.toml crabster-api/
COPY crabster-hls/Cargo.toml crabster-hls/
COPY crabster-analytics/Cargo.toml crabster-analytics/
COPY crabster-health/Cargo.toml crabster-health/
COPY crabster-cluster/Cargo.toml crabster-cluster/
COPY crabster-db/Cargo.toml crabster-db/

RUN mkdir -p crabster-core/src crabster-api/src crabster-hls/src \
    crabster-analytics/src crabster-health/src crabster-cluster/src crabster-db/src \
    src tests && \
    echo "fn main() {}" > src/main.rs && \
    for d in crabster-core crabster-api crabster-hls crabster-analytics crabster-health crabster-cluster crabster-db; do \
        echo "// placeholder" > $d/src/lib.rs; \
    done

RUN cargo build --release 2>/dev/null || true

COPY src src/
COPY crabster-core/src crabster-core/src/
COPY crabster-api/src crabster-api/src/
COPY crabster-hls/src crabster-hls/src/
COPY crabster-analytics/src crabster-analytics/src/
COPY crabster-health/src crabster-health/src/
COPY crabster-cluster/src crabster-cluster/src/
COPY crabster-db/src crabster-db/src/

RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/crabster /usr/local/bin/crabster

EXPOSE 8000 8001 8002
ENTRYPOINT ["crabster"]
