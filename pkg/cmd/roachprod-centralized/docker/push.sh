#!/bin/bash

# Copyright 2021 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

set -euo pipefail

# root is the absolute path to the root directory of the repository.
root="$(cd ../../../../ &> /dev/null && pwd)"
source "$root/build/teamcity-bazel-support.sh"  # For BAZEL_IMAGE

if [[ -z ${OWNER+x} ]]; then
    OWNER=cockroachdb
fi
if [[ -z ${REPO+x} ]]; then
    REPO=cockroach
fi

SHA=$(git rev-parse --short HEAD)
IMAGE=us-central1-docker.pkg.dev/cockroach-testeng-infra/roachprod/roachprod-centralized

gcloud --project cockroach-testeng-infra builds submit \
  --substitutions=_BAZEL_IMAGE=$BAZEL_IMAGE,_SHA=$SHA,_OWNER=$OWNER,_REPO=$REPO \
  --timeout=30m
  
# echo "Building and pushing image ${IMAGE}:${SHA}"

# # Clean up any previous images or manifests
# podman image rm -i "${IMAGE}:${SHA}"
# podman manifest rm -i "${IMAGE}:${SHA}"

# # Create a manifest for the image
# podman manifest create "${IMAGE}:${SHA}"

# # Build the image for amd64 and arm64 platforms
# podman buildx build \
#   --manifest "${IMAGE}:${SHA}" \
#   --layers \
#   --format docker \
#   --cache-from "${IMAGE}-cache" \
#   --cache-to "${IMAGE}-cache" \
#   --build-arg BAZEL_IMAGE=$BAZEL_IMAGE \
#   --file Dockerfile \
#   $root

# # Push the image to the registry
# podman manifest push "${IMAGE}:${SHA}"

# echo "Image pushed to ${IMAGE}:${SHA}"