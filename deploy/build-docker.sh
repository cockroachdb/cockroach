#!/bin/bash

REQUIRED_IMAGES="cockroachdb/docker_base"

# Verify docker installation.
source "$(dirname $0)/verify-docker.sh"

# Create the docker cockroach image.
echo "Building Docker Cockroach image..."
for IMG in ${REQUIRED_IMAGES}; do
  if [ $(docker images | awk '{ print $1 }' | grep -c "${IMG}") -eq 0 ]; then
    docker build -t "${IMG}" "github.com/${IMG}.git"
  fi
done

docker build -t "cockroachdb/cockroach" "$(dirname $0)/.."
