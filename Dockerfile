# Use the official Rust image as the base image
FROM rust:latest

# Install necessary packages for cross-compilation
RUN apt-get update && apt-get install -y \
    pkg-config


# Create a new directory for the project
WORKDIR /usr/src/myapp

# Copy the project files into the container
COPY . .

# Build the project for both targets
RUN cargo build --release
ENTRYPOINT ["/usr/src/myapp/target/release/feats_of_distance"]