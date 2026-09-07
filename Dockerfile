FROM rust:bookworm AS builder

RUN apt-get update && apt-get install -y \
    build-essential \
    pkg-config \
    libssl-dev \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*
ENV RUSTFLAGS="-C target-cpu=broadwell"

WORKDIR /app

COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo "fn main() {}" > src/main.rs && cargo build --release && rm -rf src

COPY src ./src
RUN touch src/main.rs && cargo build --release && strip /app/target/release/kcs-monitor

FROM debian:bookworm-slim AS runner

RUN apt-get update && apt-get install -y \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && update-ca-certificates

WORKDIR /app

RUN addgroup --system --gid 1000 appuser && adduser --system --uid 1000 --gid 1000 appuser

COPY --from=builder /app/target/release/kcs-monitor /app/kcs-monitor

USER appuser

CMD ["/app/kcs-monitor"]
