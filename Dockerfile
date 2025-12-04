# Base image
FROM rust:1.81-slim

LABEL Author="Fazal Kareem" Version="v0.3.0"

ENV CARGO_TERM_COLOR=always
ENV PATH="/usr/local/cargo/bin:${PATH}"

RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config libssl-dev build-essential git procps ca-certificates \
    python3 python3-pip python3-dev \
 && rm -rf /var/lib/apt/lists/*

RUN pip3 install --no-cache-dir --break-system-packages \
    pandas numpy scipy astropy lxml matplotlib uncertainties

WORKDIR /opt/candy_picker_rs
COPY . ./

# Show what got copied (helps diagnose missing src/bin)
RUN ls -R src

# Build *all* binaries and install them
RUN cargo build --release --bins \
 && install -m 0755 target/release/candy_picker_rs /usr/local/bin/ \
 && install -m 0755 target/release/csv_candypicker /usr/local/bin/

ENTRYPOINT ["candy_picker_rs"]
