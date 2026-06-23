# Linux build/test environment for zenoh (C++23 named modules).
#
# Ships two toolchains so the module build is validated on both, mirroring the
# project's real target:
#   - clang + libc++  -> primary (matches the Linux release CI target)
#   - g++-14          -> second compiler, catches portability/ODR issues
#
# Build:  docker build -t zenoh-linux-ci -f docker/Dockerfile .
# Run:    docker run --rm -v "$PWD":/work -w /work zenoh-linux-ci scripts/ci.sh
FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive
ARG LLVM_VERSION=19

RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates wget gnupg lsb-release software-properties-common \
        ninja-build python3 python3-pip git make \
        gcc-14 g++-14 \
    && rm -rf /var/lib/apt/lists/*

# CMake >= 3.28 is required for C++23 named-module support (FILE_SET CXX_MODULES).
# The Ubuntu 24.04 package is 3.28, but we pull the latest from PyPI for the most
# robust module dependency scanning.
RUN pip3 install --no-cache-dir --break-system-packages "cmake>=3.28"

# Clang + libc++ from the official LLVM apt repository.
RUN wget -qO /tmp/llvm.sh https://apt.llvm.org/llvm.sh \
    && chmod +x /tmp/llvm.sh \
    && /tmp/llvm.sh ${LLVM_VERSION} \
    && apt-get update && apt-get install -y --no-install-recommends \
        clang-${LLVM_VERSION} clang-tools-${LLVM_VERSION} \
        libc++-${LLVM_VERSION}-dev libc++abi-${LLVM_VERSION}-dev \
    && rm -rf /var/lib/apt/lists/* /tmp/llvm.sh

# Stable, unversioned compiler names that the CMake presets reference.
RUN update-alternatives --install /usr/bin/clang   clang   /usr/bin/clang-${LLVM_VERSION}   100 \
    && update-alternatives --install /usr/bin/clang++ clang++ /usr/bin/clang++-${LLVM_VERSION} 100 \
    && update-alternatives --install /usr/bin/gcc    gcc     /usr/bin/gcc-14                  100 \
    && update-alternatives --install /usr/bin/g++    g++     /usr/bin/g++-14                  100

WORKDIR /work
CMD ["bash"]
