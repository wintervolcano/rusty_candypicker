# Base image
FROM rust:1.81-slim

# Metadata
LABEL Author="Fazal Kareem" \
      Version="v0.3.0"

# Environment variables
ENV CARGO_TERM_COLOR=always
ENV PATH="/usr/local/cargo/bin:${PATH}"

# Install dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
        pkg-config \
        libssl-dev \
        build-essential \
        git \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Create app directory
WORKDIR /opt/candy_picker_rs

# Copy project source into container
COPY . /opt/candy_picker_rs

# Build the binary
RUN cargo build --release \
    && cp target/release/candy_picker_rs /usr/local/bin/

# Set entrypoint
ENTRYPOINT ["candy_picker_rs"]
