FROM rust:bullseye AS builder

WORKDIR /usr/src/app

COPY . .

RUN cargo build --release

FROM debian:bullseye-slim

WORKDIR /app

RUN apt-get update && apt-get install -y openssl ca-certificates && rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/src/app/target/release/log_vault /usr/local/bin/broker

EXPOSE 8080

CMD ["broker"]