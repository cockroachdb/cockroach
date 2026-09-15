#!/usr/bin/env bash

# Copyright 2026 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

# Build and stage the experimental Ubuntu 24.04 Docker image (RE-1123) as
# "<version>-noble" in the staged GAR repository. The publish step promotes
# that tag to the customer-facing registries; see
# build/teamcity/internal/cockroach/release/publish/publish-staged-cockroach-release-ubuntu.sh
#
# This is deliberately a self-contained sibling of
# build-cockroach-release-docker.sh rather than a parameterization of it. On
# master the two share one script via DOCKER_* env knobs, but the older release
# branches this is backported to build their UBI image per architecture inside
# the blocking build-linux matrix, so a shared script would mean editing that
# blocking path. Keeping the Ubuntu build standalone means the UBI path is
# untouched on every branch and this stays a true non-blocking leaf: when it
# breaks, no release is affected.
#
# The scope is intentionally narrow: amd64 + arm64 (s390x is not a target for
# this image) and no FIPS (a UBI-only offering). The telemetry-disabled IBM
# variant likewise stays on the UBI path, so this script has no
# COCKROACH_ARCHIVE_PREFIX / TELEMETRY_DISABLED knobs to get wrong.

set -euo pipefail

dir="$(dirname $(dirname $(dirname $(dirname $(dirname "${0}")))))"
source "$dir/release/teamcity-support.sh"

# Canonical's trademark policy restricts use of the "Ubuntu" mark in a URL, so
# the published tag uses the 24.04 release codename instead.
tag_suffix="-noble"
arches=(amd64 arm64)

tc_start_block "Variable Setup"
version=$(grep -v "^#" "$dir/../pkg/build/version.txt" | head -n1)
version_label=$(echo "${version}" | sed -e 's/^v//' | cut -d- -f 1)

if [[ -z "${DRY_RUN}" ]] ; then
  gcs_bucket="cockroach-release-artifacts-staged-prod"
  gcr_staged_repository="us-docker.pkg.dev/releases-prod/cockroachdb-staged-releases/cockroach"
else
  gcs_bucket="cockroach-release-artifacts-staged-dryrun"
  gcr_staged_repository="us-docker.pkg.dev/releases-dev-356314/cockroachdb-staged-releases/cockroach"
fi

# With WIF (GitHub Actions), credentials are handled via the environment.
# With TeamCity, use the JSON key env vars.
if [[ -n "${GCS_CREDENTIALS_PROD:-}" || -n "${GCS_CREDENTIALS_DEV:-}" ]]; then
  if [[ -z "${DRY_RUN}" ]] ; then
    export gcp_credentials="$GCS_CREDENTIALS_PROD"
  else
    export gcp_credentials="$GCS_CREDENTIALS_DEV"
  fi
else
  export gcp_credentials=""
fi
tc_end_block "Variable Setup"


tc_start_block "Download and extract tarballs"
google_credentials="${gcp_credentials}"
log_into_gcloud

tmpdir=$(mktemp -d)
trap "rm -rf $tmpdir; remove_files_on_exit" EXIT

# Lay the build context out as ${arch}/ subdirectories, which is what the
# ${TARGETARCH}-keyed COPY lines in Dockerfile.ubuntu expect.
context="$tmpdir/context"
mkdir -p "$context"
cp build/deploy/Dockerfile.ubuntu "$context/Dockerfile"

for arch in "${arches[@]}"; do
  archive="cockroach-${version}.linux-${arch}.tgz"
  gcloud storage cp "gs://$gcs_bucket/$archive" "$tmpdir/$archive"
  # Extract into a staging directory, then copy the files the Dockerfile needs
  # into the ${arch}/ subdirectory of the build context.
  staging="$tmpdir/staging-linux-${arch}"
  mkdir -p "$staging"
  tar \
    --directory="$staging" \
    --extract \
    --file="$tmpdir/$archive" \
    --ungzip \
    --ignore-zeros \
    --strip-components=1
  mkdir -p "$context/${arch}"
  cp build/deploy/cockroach.sh "$context/${arch}/"
  cp "$staging/cockroach" "$context/${arch}/"
  cp "$staging"/lib/libgeos.so "$staging"/lib/libgeos_c.so "$context/${arch}/"
  cp LICENSE licenses/THIRD-PARTY-NOTICES.txt "$context/${arch}/"
done
tc_end_block "Download and extract tarballs"


tc_start_block "Build and push multi-arch docker image"
docker_login_gcr "$gcr_staged_repository" "${gcp_credentials:-}"

# Create a buildx builder for multi-platform builds. The name is distinct from
# the UBI script's so the two can run concurrently on the same runner.
docker buildx rm "release-builder-ubuntu-$$" 2>/dev/null || true
docker buildx create --name "release-builder-ubuntu-$$" --use
cleanup_buildx() { docker buildx rm "release-builder-ubuntu-$$" || true; }
trap "cleanup_buildx; rm -rf $tmpdir; remove_files_on_exit" EXIT

gcr_tag="${gcr_staged_repository}:${version}${tag_suffix}"
docker buildx build --label version="$version_label" --pull --push --no-cache \
  --platform linux/amd64,linux/arm64 \
  --tag "$gcr_tag" "$context"
tc_end_block "Build and push multi-arch docker image"


tc_start_block "Verify docker images"
error=0
for arch in "${arches[@]}"; do
    tc_start_block "Verify $gcr_tag on $arch"
    # The trailing false/false are fips_build and telemetry_disabled: this
    # image is never FIPS and always telemetry-enabled.
    if ! verify_docker_image "$gcr_tag" "linux/$arch" "$BUILD_VCS_NUMBER" "$version" false false; then
      error=1
    fi
    tc_end_block "Verify $gcr_tag on $arch"
done

if [ $error = 1 ]; then
  echo "ERROR: Docker image verification failed, see logs above"
  exit 1
fi
tc_end_block "Verify docker images"
