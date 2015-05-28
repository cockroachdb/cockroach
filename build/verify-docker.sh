#!/bin/bash

set -u

# Verify that Docker is installed.
DOCKER="docker"
if [[ ! $(type -P "$DOCKER") ]]; then
  echo "Docker executable not found!"
  echo "Installation instructions at https://docs.docker.com/installation/"
  exit 1
fi

# Verify docker is reachable.
OUT=$(($DOCKER images > /dev/null) 2>&1)
if [[ ! $? == 0 ]]; then
  echo "Docker is not reachable. Did you follow installation instructions?"
  echo "'docker images': $OUT"
  exit 1
fi
