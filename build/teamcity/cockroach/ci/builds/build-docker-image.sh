#!/usr/bin/env bash
set -euo pipefail

dir="$(dirname $(dirname $(dirname $(dirname $(dirname "${0}")))))"
source "$dir/teamcity-support.sh"  # For $root

# The artifacts dir should match up with that supplied by TC.
artifacts=$PWD/artifacts
mkdir -p "${artifacts}"
chmod o+rwx "${artifacts}"

tc_start_block "Copy cockroach binary and dependency files to build/deploy"

cp upstream_artifacts/cockroach build/deploy
cp lib.docker_amd64/libgeos.so lib.docker_amd64/libgeos_c.so build/deploy
cp -r licenses build/deploy/

chmod u+x build/deploy/cockroach

tc_end_block "Copy cockroach binary and dependency files to build/deploy"

tc_start_block "Build and save docker image to artifacts"

docker_image_tar_name="cockroach-docker-image.tar"
docker_tag="demo_docker_image/demo"
docker build \
  --no-cache \
  --tag="$docker_tag" \
  build/deploy

docker save -o "${artifacts}/${docker_image_tar_name}" "$docker_tag"

tc_end_block "Build and save docker image to artifacts"


