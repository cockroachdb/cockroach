#!/bin/bash
set -xeuo pipefail

TARGET=$1
TAG=$(date +%Y%m%d-%H%M%S)
docker buildx create --use
docker buildx build --push --platform linux/amd64,linux/arm64 -t cockroachdb/acceptance-gss-$TARGET:$TAG ./$TARGET
