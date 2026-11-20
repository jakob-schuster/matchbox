FROM lukemathwalker/cargo-chef:latest-rust-1 AS chef
WORKDIR /app

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
COPY . .
RUN apt-get -y update
RUN apt-get install -y cmake
RUN cargo build --release --bin matchbox

FROM debian:trixie-slim AS runtime
WORKDIR /app
COPY --from=builder /app/target/release/matchbox /usr/local/bin
ENTRYPOINT ["/usr/local/bin/matchbox"]