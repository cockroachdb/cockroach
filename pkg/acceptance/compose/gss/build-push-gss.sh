#!/bin/bash

# Copyright 2022 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

set -xeuo pipefail

TARGET=$1
TAG=$(date +%Y%m%d-%H%M%S)
docker buildx create --use
docker buildx build --push --platform linux/amd64,linux/arm64 -t us-east1-docker.pkg.dev/crl-ci-images/cockroach/acceptance-gss-$TARGET:$TAG ./$TARGET
