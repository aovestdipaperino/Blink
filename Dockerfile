# Leveraging the pre-built Docker images with
# cargo-chef and the Rust toolchain
FROM lukemathwalker/cargo-chef:latest-rust-1.84.1-slim AS chef
WORKDIR /app

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json
# Build application
COPY src src
COPY Cargo.toml Cargo.toml
# Build both profiled and non-profiled binaries
RUN cargo build --release
RUN cargo build --release --features profiled --bin blink
# Rename the profiled binary
RUN mv /app/target/release/blink /app/target/release/blink-instrumented
# Rebuild the standard binary (since it was overwritten)
RUN cargo build --release

# We do not need the Rust toolchain to run the binary!
FROM debian:bookworm-slim AS runtime
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /app/target/release/blink /app/blink
COPY --from=builder /app/target/release/blink-instrumented /app/blink-instrumented
COPY settings.yaml /app/settings.yaml

# Create startup script that fetches the internal IP and starts the appropriate application
RUN echo '#!/bin/bash\n\
    set -e\n\
    \n\
    # Fetch internal IP from Google Cloud metadata server\n\
    KAFKA_HOSTNAME=$(curl -H "Metadata-Flavor: Google" http://169.254.169.254/computeMetadata/v1/instance/network-interfaces/0/ip 2>/dev/null) && export KAFKA_HOSTNAME\n\
    \n\
    # Fetch GCP project ID from Google Cloud metadata server for GCP logging\n\
    if GCP_PROJECT_ID=$(curl -H "Metadata-Flavor: Google" http://169.254.169.254/computeMetadata/v1/project/project-id 2>/dev/null); then\n\
    export GCP_PROJECT_ID\n\
    echo "GCP project ID detected: $GCP_PROJECT_ID - GCP logging will be enabled"\n\
    else\n\
    echo "GCP metadata server not available - running with console-only logging"\n\
    fi\n\
    \n\
    # Select binary based on INSTRUMENTED_BUILD environment variable\n\
    if [ "$INSTRUMENTED_BUILD" = "true" ]; then\n\
    echo "Starting Blink with instrumentation enabled..."\n\
    echo "Profile data available at http://localhost:3030/profile-counts"\n\
    exec /app/blink-instrumented "$@"\n\
    else\n\
    echo "Starting Blink with standard performance..."\n\
    exec /app/blink "$@"\n\
    fi' > /app/start.sh && chmod +x /app/start.sh

ENTRYPOINT ["/app/start.sh"]
