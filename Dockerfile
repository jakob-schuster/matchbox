FROM rust:1.87

WORKDIR /app
COPY . .
RUN cargo build
