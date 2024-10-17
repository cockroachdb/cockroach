#!/usr/bin/env bash

# Copyright 2021 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

set -euxo pipefail

# The first and only parameter is the name of the architecture the image is being built for.
# This should be the format Docker expects for the platform flag minus linux/ - i.e., either
# amd64 or arm64. Old TC configs will run this file directly and not supply a platform, so
# default to amd64.
build_arch=${1:-amd64}

dir="$(dirname $(dirname $(dirname $(dirname $(dirname "${0}")))))"
source "$dir/teamcity-support.sh"  # For $root

# The artifacts dir should match up with that supplied by TC.
artifacts=$PWD/artifacts
mkdir -p "${artifacts}"
chmod o+rwx "${artifacts}"

tc_start_block "Copy cockroach binary and dependency files to build/deploy"

# Get the cockroach binary from Build (Linux ${arch})
# Artifacts rules:
# bazel-bin/pkg/cmd/cockroach/cockroach_/cockroach=>upstream_artifacts
# bazel-bin/c-deps/libgeos/lib/libgeos.so=>upstream_artifacts
# bazel-bin/c-deps/libgeos/lib/libgeos_c.so=>upstream_artifacts
cp upstream_artifacts/cockroach\
   upstream_artifacts/libgeos.so \
   upstream_artifacts/libgeos_c.so \
   build/deploy

cp LICENSE licenses/THIRD-PARTY-NOTICES.txt build/deploy/

chmod 755 build/deploy/cockroach

tc_end_block "Copy cockroach binary and dependency files to build/deploy"

tc_start_block "Build and save docker image to artifacts"

docker_image_tar_name="cockroach-docker-image.tar"

docker_tag="cockroachdb/cockroach-ci"

# We have to always pull here because this runner may have been used to build
# a different architecture's docker image. If that's the case, the cache will
# return the cached version of the UBI base image (which will be for the wrong
# architecture), then build will use it and fail because it's for the wrong
# architecture. The cache is really stupid, in other words.

docker build \
  --no-cache \
  --platform=linux/${build_arch} \
  --tag="$docker_tag" \
  --memory 30g \
  --memory-swap -1 \
  --pull \
  build/deploy

docker save "$docker_tag" | gzip > "${artifacts}/${docker_image_tar_name}".gz

cp upstream_artifacts/cockroach "${artifacts}"/cockroach

tc_end_block "Build and save docker image to artifacts"
