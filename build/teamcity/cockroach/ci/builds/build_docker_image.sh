#!/usr/bin/env bash
set -euo pipefail

dir="$(dirname $(dirname $(dirname $(dirname $(dirname "${0}")))))"
source "$dir/teamcity-support.sh"  # For $root

# The artifacts dir should match up with that supplied by TC.
artifacts=$PWD/artifacts
mkdir -p "${artifacts}"
chmod o+rwx "${artifacts}"

tc_start_block "Copy cockroach binary and dependency files to build/deploy"

# Get the cockroach binary from Build (Linux x86_64)
# Artifacts rules:
# bazel-bin/pkg/cmd/cockroach/cockroach_/cockroach=>upstream_artifacts
# bazel-bin/c-deps/libgeos/lib/libgeos.so=>upstream_artifacts
# bazel-bin/c-deps/libgeos/lib/libgeos_c.so=>upstream_artifacts
cp upstream_artifacts/cockroach\
   upstream_artifacts/libgeos.so \
   upstream_artifacts/libgeos_c.so \
   build/deploy

cp -r licenses build/deploy/

chmod 755 build/deploy/cockroach

tc_end_block "Copy cockroach binary and dependency files to build/deploy"

tc_start_block "Build and save docker image to artifacts"

docker_image_tar_name="cockroach-docker-image.tar"

docker_tag="cockroachdb/cockroach-ci"

docker build \
  --no-cache \
  --tag="$docker_tag" \
  --memory 30g \
  --memory-swap -1 \
  build/deploy

docker save "$docker_tag" | gzip > "${artifacts}/${docker_image_tar_name}".gz

cp upstream_artifacts/cockroach "${artifacts}"/cockroach

tc_end_block "Build and save docker image to artifacts"
