#!/usr/bin/env bash
set -euo pipefail

find . -ls

# Matching the version name regex from within the cockroach code except
# for the `metadata` part at the end because Docker tags don't support
# `+` in the tag name.
# https://github.com/cockroachdb/cockroach/blob/4c6864b44b9044874488cfedee3a31e6b23a6790/pkg/util/version/version.go#L75
build_name="$(echo "${NAME}" | grep -E -o '^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-[-.0-9A-Za-z]+)?$')"
#                                         ^major           ^minor           ^patch         ^preRelease
if [[ -z "$build_name" ]] ; then
    echo "Invalid NAME \"${NAME}\". Must be of the format \"vMAJOR.MINOR.PATCH(-PRERELEASE)?\"."
    exit 1
fi

dir="$(dirname $(dirname $(dirname $(dirname $(dirname "${0}")))))"
source "$dir/teamcity-support.sh"  # For $root

# The artifacts dir should match up with that supplied by TC.
artifacts=$PWD/artifacts
mkdir -p "${artifacts}"
chmod o+rwx "${artifacts}"

tc_start_block "Copy cockroach binary and dependency files to build/deploy"

# Get the cockroach binary from Build (Linux x86_64)
# Artifacts rules: bazel-bin/pkg/cmd/cockroach/cockroach_/cockroach=>upstream_artifacts
cp upstream_artifacts/cockroach build/deploy
cp bazel-bin/c-deps/libgeos/lib/libgeos.so bazel-bin/c-deps/libgeos/lib/libgeos_c.so build/deploy
cp -r licenses build/deploy/

chmod 755 build/deploy/cockroach

tc_end_block "Copy cockroach binary and dependency files to build/deploy"

tc_start_block "Build and save docker image to artifacts"

docker_image_tar_name="cockroach-docker-image.tar"
dockerhub_repository="docker.io/cockroachdb/cockroach-misc"
docker_tag="${dockerhub_repository}:${build_name}-demo"

docker build \
  --no-cache \
  --tag="$docker_tag" \
  build/deploy

docker save "$docker_tag" | gzip > "${artifacts}/${docker_image_tar_name}".gz

tc_end_block "Build and save docker image to artifacts"

tc_start_block "Push docker image"

configure_docker_creds
docker_login_with_google
docker_login

#docker push "${docker_tag}"

tc_end_block "Push docker image"

