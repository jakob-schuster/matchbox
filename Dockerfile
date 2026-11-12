FROM rust:latest

WORKDIR /app
COPY . .

RUN apt-get -y update
RUN apt-get install -y cmake

RUN cargo build --release
