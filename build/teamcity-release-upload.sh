#!/usr/bin/env bash
set -euxo pipefail

export BUILDER_HIDE_GOPATH_SRC=1

mkdir -p artifacts

build/builder.sh go install ./pkg/cmd/release

build/builder.sh env \
  DOCKER_EMAIL="$DOCKER_EMAIL" \
  DOCKER_AUTH="$DOCKER_AUTH" \
  TC_BUILD_BRANCH="$TC_BUILD_BRANCH" \
  release
