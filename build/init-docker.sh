#!/bin/bash

set -eu

if [ "${DOCKER_HOST-}" = "" -a "$(uname)" = "Darwin" ]; then
  if ! type -P "docker-machine" >& /dev/null; then
    echo "docker-machine not found!"
    exit 1
  fi
  echo "docker-machine env # initializing DOCKER_* env variables"
  eval $(docker-machine env default)
fi

# Verify that Docker is installed.
DOCKER="docker"
if [[ ! $(type -P "$DOCKER") ]]; then
  echo "Docker executable not found!"
  echo "Installation instructions at https://docs.docker.com/installation/"
  exit 1
fi

# Verify docker is reachable.
OUT=$(($DOCKER images  > /dev/null) 2>&1) || (
  echo "Docker is not reachable. Is the Docker daemon running?"
  echo "'docker images': $OUT"
  exit 1
)
