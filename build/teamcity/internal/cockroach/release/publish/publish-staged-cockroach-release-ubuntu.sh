#!/usr/bin/env bash

# Copyright 2026 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


# Promote the experimental Ubuntu 24.04 Docker image (RE-1123) from the staged
# GAR repository to the customer-facing registries, tagged "<version>-noble".
#
# This is a deliberately trimmed sibling of publish-staged-cockroach-release.sh:
# the staged-release promotion (binary copy, git tagging, "latest" tags, FIPS,
# Red Hat publishing) is owned exclusively by that script and must run exactly
# once per release. This script only re-bases and republishes the Ubuntu Docker
# image, so it can run in parallel without duplicating any of that work.
#
# Like the UBI publish path, it rebuilds FROM the staged image and runs an
# in-place package upgrade so the published image carries the freshest security
# patches available at publish time — the whole reason customers asked for an
# Ubuntu base.

set -euo pipefail

dir="$(dirname $(dirname $(dirname $(dirname $(dirname $(dirname "${0}"))))))"
source "$dir/teamcity-support.sh"  # For log_into_gcloud
source "$dir/release/teamcity-support.sh"

tag_suffix="-noble"

tc_start_block "Variable Setup"
version=$(grep -v "^#" "$dir/../pkg/build/version.txt" | head -n1)
prerelease=false
if [[ $version == *"-"* ]]; then
  # Our pre-release version contains a dash symbol, e.g. v22.2.0-alpha.1
  prerelease=true
fi

if ! echo "${version}" | grep -E -o '^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-[-.0-9A-Za-z]+)?$'; then
  #                                    ^major           ^minor           ^patch         ^preRelease
  echo "Invalid version \"${version}\". Must be of the format \"vMAJOR.MINOR.PATCH(-PRERELEASE)?\"."
  exit 1
fi

if [[ -z "${DRY_RUN}" ]] ; then
  if [[ $prerelease == false ]] ; then
    dockerhub_repository="docker.io/cockroachdb/cockroach"
  else
    dockerhub_repository="docker.io/cockroachdb/cockroach-unstable"
  fi
  gcr_staged_repository="us-docker.pkg.dev/releases-prod/cockroachdb-staged-releases/cockroach"
  gcr_repository="us-docker.pkg.dev/cockroach-cloud-images/cockroachdb/cockroach"
else
  dockerhub_repository="docker.io/cockroachdb/cockroach-misc"
  gcr_staged_repository="us-docker.pkg.dev/releases-dev-356314/cockroachdb-staged-releases/cockroach"
  gcr_repository="us-docker.pkg.dev/releases-dev-356314/cockroachdb-staged-releases/cockroach-test"
fi

# With WIF (GitHub Actions), credentials are handled via the environment.
# With TeamCity, use the JSON key env vars.
if [[ -n "${CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE:-}" ]]; then
  gcr_staged_credentials=""
  gcr_credentials=""
else
  if [[ -z "${DRY_RUN}" ]] ; then
    gcr_staged_credentials="$GCS_CREDENTIALS_PROD"
    gcr_credentials="$GOOGLE_COCKROACH_CLOUD_IMAGES_COCKROACHDB_CREDENTIALS"
  else
    gcr_staged_credentials="$GCS_CREDENTIALS_DEV"
    gcr_credentials="${GOOGLE_COCKROACH_RELEASE_CREDENTIALS:-$GCS_CREDENTIALS_DEV}"
  fi
fi
tc_end_block "Variable Setup"


tc_start_block "Verify staged Ubuntu image"
# Make sure the staged Ubuntu image exists and matches this version/SHA before
# we promote it.
docker_login_gcr "$gcr_staged_repository" "${gcr_staged_credentials:-}"
verify_docker_image "${gcr_staged_repository}:${version}${tag_suffix}" "linux/amd64" "$BUILD_VCS_NUMBER" "$version" false false
tc_end_block "Verify staged Ubuntu image"


tc_start_block "Setup dockerhub credentials"
configure_docker_creds
docker_login
tc_end_block "Setup dockerhub credentials"


tc_start_block "Make and push Ubuntu docker image"
dockerhub_tag="${dockerhub_repository}:${version}${tag_suffix}"
gcr_tag="${gcr_repository}:${version}${tag_suffix}"

# Create a buildx builder for multi-platform builds.
docker buildx rm "release-builder-$$" 2>/dev/null || true
docker buildx create --name "release-builder-$$" --use
cleanup_buildx() { docker buildx rm "release-builder-$$" || true; }
tmpdir=$(mktemp -d)
trap 'cleanup_buildx; rm -rf "$tmpdir"; remove_files_on_exit' EXIT

# The staged image is already multi-arch; buildx pulls the right platform
# automatically. Refresh packages so the published image carries the latest
# security patches at publish time.
cat > "$tmpdir/Dockerfile" <<DOCKERFILE
FROM ${gcr_staged_repository}:${version}${tag_suffix}
RUN apt-get update && apt-get upgrade -y && apt-get clean && rm -rf /var/lib/apt/lists/*
DOCKERFILE

# Build and push the multi-arch image to DockerHub. The staged repo (source)
# is on us-docker.pkg.dev and DockerHub (destination) is on docker.io, so both
# logins can coexist.
docker_login_gcr "$gcr_staged_repository" "${gcr_staged_credentials:-}"
docker buildx build --pull --push --no-cache \
  --platform linux/amd64,linux/arm64 \
  --tag "$dockerhub_tag" "$tmpdir"

# Copy the multi-arch manifest from DockerHub to GCR. These are on different
# hostnames so both logins can coexist.
docker_login_gcr "$gcr_repository" "${gcr_credentials:-}"
docker buildx imagetools create -t "$gcr_tag" "$dockerhub_tag"
tc_end_block "Make and push Ubuntu docker image"


tc_start_block "Verify Ubuntu docker images"
error=0
for img in "$dockerhub_tag" "$gcr_tag"; do
  for platform_name in amd64 arm64; do
    tc_start_block "Verify $img on $platform_name"
    if ! verify_docker_image "$img" "linux/$platform_name" "$BUILD_VCS_NUMBER" "$version" false false; then
      error=1
    fi
    tc_end_block "Verify $img on $platform_name"
  done
done

if [ $error = 1 ]; then
  echo "ERROR: Docker image verification failed, see logs above"
  exit 1
fi
tc_end_block "Verify Ubuntu docker images"
