#!/bin/bash

set -euxo pipefail

GOARCH=${GOARCH:-amd64}

VERSION_TAG="$(git rev-parse HEAD)"
VERSION_LINK_FLAG="main.gitCommit=${VERSION_TAG}"

CGO_ENABLED=0 GOOS=linux GOARCH=$GOARCH go build -a -ldflags "-extldflags '-static' -X ${VERSION_LINK_FLAG}" .
