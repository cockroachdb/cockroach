#!/bin/bash

set -eu

if [ "${DOCKER_HOST:-}" = "" -a "$(uname)" = "Darwin" ]; then
    if ! type -P "boot2docker" >& /dev/null; then
	echo "boot2docker not found!"
	exit 1
    fi
    echo "boot2docker shellinit # initializing DOCKER_* env variables"
    eval $(boot2docker shellinit 2>/dev/null)
fi

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
