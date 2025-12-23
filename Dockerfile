FROM rust:1.87

WORKDIR /app
COPY . .

RUN apt-get -y update
RUN apt-get install -y cmake

RUN cargo build --release
