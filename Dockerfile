# syntax=docker/dockerfile:1

########## Builder ##########
FROM rust:bookworm AS builder

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        pkg-config \
        libssl-dev \
        ca-certificates \
    && update-ca-certificates \
    && rm -rf /var/lib/apt/lists/*

ENV RUSTFLAGS="-C target-cpu=broadwell"

WORKDIR /app

# Кэширование зависимостей: собираем пустой проект с реальными манифестами
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo "fn main() {}" > src/main.rs \
    && cargo build --release \
    && rm -rf src

# Настоящий исходный код
COPY src ./src
RUN touch src/main.rs \
    && cargo build --release \
    && strip /app/target/release/kcs-monitor

########## Runner ##########
FROM gcr.io/distroless/cc-debian12 AS runner

WORKDIR /app

# CA-сертификаты копируем из builder (в distroless их нет)
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder /app/target/release/kcs-monitor /app/kcs-monitor

# В distroless есть встроенный непривилегированный пользователь nonroot (uid 65532)
USER nonroot

ENTRYPOINT ["/app/kcs-monitor"]
