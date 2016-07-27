#!/bin/bash

set -eu

# TODO(tamird): remove this when https://github.com/docker/engine-api/pull/320
# is merged.
export DOCKER_HOST=unix:///var/run/docker.sock

# Verify that Docker is installed.
DOCKER="docker"
if [[ ! $(type -P "$DOCKER") ]]; then
  echo "Docker executable not found!"
  echo "Installation instructions at https://docs.docker.com/installation/"
  exit 1
fi

# Verify docker is reachable.
OUT=$(($DOCKER images > /dev/null) 2>&1) || (
  echo "Docker is not reachable. Is the Docker daemon running?"
  echo "'docker images': $OUT"
  exit 1
)
