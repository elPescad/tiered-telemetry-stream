FROM rust:latest AS builder

WORKDIR /usr/src/app
COPY . .
RUN cargo build --release

FROM debian:bookworm-slim

WORKDIR /app

# Clean single-line apt run to eliminate parser formatting issues
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates libssl3 openssl && rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/src/app/target/release/log_vault /usr/local/bin/broker

EXPOSE 8080

CMD ["broker"]