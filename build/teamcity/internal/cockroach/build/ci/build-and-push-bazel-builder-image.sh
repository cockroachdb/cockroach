#!/usr/bin/env bash
set -xeuo pipefail

TAG=$(date +%Y%m%d-%H%M%S)
docker buildx create --name "builder-$TAG" --use
docker buildx build --push --platform linux/amd64,linux/arm64 -t "cockroachdb/bazel:$TAG" -t "cockroachdb/bazel:latest-do-not-use" build/bazelbuilder
