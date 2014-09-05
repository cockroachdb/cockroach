#!/bin/bash

# Verify docker installation.
source ./verify-docker.sh

# Create the docker cockroach image.
echo "Building Docker Cockroach image..."
docker build -t="cockroachdb/cockroach" ../
