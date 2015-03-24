#!/bin/bash
set -e
cd "$(dirname $0)/.."

# Verify docker installation.
./build/verify-docker.sh

cp -p GLOCKFILE build/devbase
# Creating this here helps to not break the cache during deployment runs.
mkdir -p build/deploy/build

# Create the docker cockroach image.
echo "Building Docker Cockroach images..."
docker build -t "cockroachdb/cockroach-devbase" ./build/devbase
docker build -t "cockroachdb/cockroach-dev" .
