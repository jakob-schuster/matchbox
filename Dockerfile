FROM rust:1.87

LABEL org.opencontainers.image.version="v0.2.0"
LABEL org.opencontainers.image.authors="schuster.j@wehi.edu.au"

RUN apt-get -y update
RUN apt-get install -y cmake
ENV CARGO_NET_GIT_FETCH_WITH_CLI=true
RUN cargo install matchbox-cli
