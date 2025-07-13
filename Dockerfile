# ----------------------------------------------------------------------------
# 1. Pick a base image: A stable, long-term support version of Ubuntu
# ----------------------------------------------------------------------------
FROM ubuntu:22.04

# Set a non-interactive frontend to prevent apt-get from asking questions
ENV DEBIAN_FRONTEND=noninteractive

# ----------------------------------------------------------------------------
# 2. Install build dependencies using Ubuntu's package manager (apt-get)
#    This replaces the need for vcpkg inside the container.
# ----------------------------------------------------------------------------
RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    git \
    curl \
    pkg-config \
    libssl-dev \
    libboost-dev \
    libboost-system-dev \
    libboost-thread-dev \
    zlib1g-dev \
    nlohmann-json3-dev \
    rapidjson-dev \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    \
    && curl https://sh.rustup.rs -sSf | sh -s -- -y --default-toolchain nightly \
    && apt-get purge -y curl

# Ensure the nightly toolchain is on PATH
ENV PATH="/root/.cargo/bin:${PATH}"

# ----------------------------------------------------------------------------
# 3. Copy your project's source code into the container image
# ----------------------------------------------------------------------------
WORKDIR /app
COPY . .

# ----------------------------------------------------------------------------
# 4. Configure the project with CMake and build the executable
# ----------------------------------------------------------------------------
RUN mkdir build \
    && cmake -S . -B build -DCMAKE_BUILD_TYPE=Release \
    && cmake --build build -- -j$(nproc)

# ----------------------------------------------------------------------------
# 5. Set the default command to run when the container starts
# ----------------------------------------------------------------------------
# The final executable will be located at /app/build/kraken_l3_collector
ENTRYPOINT ["/app/build/kraken_l3_collector"]