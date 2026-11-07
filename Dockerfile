# syntax=docker/dockerfile:1
#
# Reference image for a rafty node.
#
# Build:  docker build -t rafty/kiwi:dev .
# Run:    docker run --rm -p 8001:8000 -e RAFTY_NODE_ID=node1 rafty/kiwi:dev
#
# The Rust build is a separate stage so the runtime image contains only the binary,
# the configuration templates and CA certificates.
FROM rust:1.99-bookworm AS builder

WORKDIR /build

# Copy manifests first so dependency compilation is cached independently of source
# changes. The workspace needs all member manifests present for `cargo` to resolve it.
COPY Cargo.toml Cargo.lock ./
COPY rafty/Cargo.toml rafty/Cargo.toml
COPY kiwi/Cargo.toml kiwi/Cargo.toml
RUN mkdir -p rafty/src kiwi/src \
    && printf 'fn main() {}\n' > kiwi/src/main.rs \
    && printf '' > rafty/src/lib.rs \
    && cargo build --release -p kiwi --bin kiwi \
    && rm -rf rafty/src kiwi/src

COPY rafty/src rafty/src
COPY kiwi/src kiwi/src
RUN cargo build --release -p kiwi --bin kiwi

FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --home-dir /var/lib/rafty --create-home rafty \
    && mkdir -p /etc/rafty /data \
    && chown -R rafty:rafty /var/lib/rafty /data

COPY --from=builder /build/target/release/kiwi /usr/local/bin/kiwi
COPY rafty/config.yaml /etc/rafty/config.yaml
COPY rafty/config.joiner.yaml /etc/rafty/config.joiner.yaml

ENV RAFTY_NODE_ID=node1 \
    RAFTY_HTTP_LISTEN=0.0.0.0:8000 \
    RAFTY_CONFIG=/etc/rafty/config.yaml \
    RUST_LOG=info

# 8000: HTTP API and health. 1001-1004: peer RPC ports used by config.yaml.
EXPOSE 8000
EXPOSE 1001 1002 1003 1004

USER rafty
WORKDIR /var/lib/rafty

HEALTHCHECK --interval=10s --timeout=2s --start-period=5s --retries=3 \
    CMD curl -fsS http://127.0.0.1:8000/health || exit 1

ENTRYPOINT ["/usr/local/bin/kiwi"]
