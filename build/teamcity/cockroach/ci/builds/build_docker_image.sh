#!/usr/bin/env bash
set -euo pipefail

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

cp -r licenses build/deploy/

chmod 755 build/deploy/cockroach

tc_end_block "Copy cockroach binary and dependency files to build/deploy"

tc_start_block "Build and save docker image to artifacts"

docker_image_tar_name="cockroach-docker-image.tar"

docker_tag="cockroachdb/cockroach-ci"

docker rmi "$(grep '^FROM ' build/deploy/Dockerfile | head -n1 | cut -d ' ' -f2)"
docker build \
  --no-cache \
  --platform=linux/${build_arch} \
  --tag="$docker_tag" \
  --memory 30g \
  --memory-swap -1 \
  build/deploy
docker rmi "$(grep -e '^FROM ' build/deploy/Dockerfile | head -n1 | cut -d ' ' -f2)"

docker save "$docker_tag" | gzip > "${artifacts}/${docker_image_tar_name}".gz

cp upstream_artifacts/cockroach "${artifacts}"/cockroach

tc_end_block "Build and save docker image to artifacts"
