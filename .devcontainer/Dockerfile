# Minimal image for building the arrow-flight-concurrency-probe diagnostic.
# Mirrors the apt package list from ci/docker/ubuntu-24.04-cpp-minimal.dockerfile
# (without LLVM, MinIO, GCS testbench, etc.) and adds Ninja.
#
# Arrow's bundled dependencies (gRPC, Protobuf, gflags) are fetched and built
# during cmake configure, so no system packages for those are needed.
FROM ubuntu:24.04

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update -y -q && \
    apt-get install -y -q --no-install-recommends \
        build-essential \
        ca-certificates \
        ccache \
        cmake \
        curl \
        gdb \
        git \
        libcurl4-openssl-dev \
        libssl-dev \
        ninja-build \
        pkg-config \
        python3 \
        tzdata \
        wget \
        zlib1g-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV PATH=/usr/lib/ccache/:$PATH \
    CCACHE_DIR=/workspaces/arrow/.ccache

WORKDIR /workspaces/arrow
