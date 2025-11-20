FROM rust:1.87

RUN apt-get -y update
RUN apt-get install -y cmake
ENV CARGO_NET_GIT_FETCH_WITH_CLI=true
RUN cargo install matchbox-cli
