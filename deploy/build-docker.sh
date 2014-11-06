#!/bin/bash
cd "$(dirname $0)/.."

# Verify docker installation.
source "./deploy/verify-docker.sh"

# Create the docker cockroach image.
echo "Building Docker Cockroach image..."
docker build -t "cockroachdb/cockroach_base" ./build
docker build -t "cockroachdb/cockroach" .
