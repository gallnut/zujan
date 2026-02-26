FROM ubuntu:24.04 AS builder

ENV DEBIAN_FRONTEND=noninteractive

# Install dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    libgrpc++-dev \
    protobuf-compiler-grpc \
    libprotobuf-dev \
    protobuf-compiler \
    liburing-dev \
    git \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy source
COPY . .

# Build
RUN cmake -B build -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTS=OFF
RUN cmake --build build -j$(nproc)

FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive

# Install runtime libraries
RUN apt-get update && apt-get install -y \
    libgrpc++-dev \
    libprotobuf-dev \
    liburing-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /data

COPY --from=builder /app/bin/zujan /usr/local/bin/zujan

ENTRYPOINT ["zujan"]
