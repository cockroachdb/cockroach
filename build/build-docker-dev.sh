#!/bin/bash
set -e
cd "$(dirname $0)/.."

# Verify docker installation.
./build/verify-docker.sh

# Create the docker cockroach image.
echo "Building Docker Cockroach images..."
docker build -t "cockroachdb/cockroach-devbase" ./build/devbase
docker build -t "cockroachdb/cockroach-dev" .
