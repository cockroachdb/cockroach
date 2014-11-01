#!/bin/bash

# Verify docker installation.
source "$(dirname $0)/verify-docker.sh"

# Create the docker cockroach image.
echo "Building Docker Cockroach image..."
docker build -t="cockroachdb/cockroach" "$(dirname $0)/../"
