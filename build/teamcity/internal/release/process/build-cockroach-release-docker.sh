#!/usr/bin/env bash

# Copyright 2023 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -euxo pipefail

dir="$(dirname $(dirname $(dirname $(dirname $(dirname "${0}")))))"
source "$dir/release/teamcity-support.sh"

tc_start_block "Variable Setup"
telemetry_disabled="${TELEMETRY_DISABLED:-false}"
cockroach_archive_prefix="${COCKROACH_ARCHIVE_PREFIX:-cockroach}"
if [[ $telemetry_disabled == true && $cockroach_archive_prefix == "cockroach" ]]; then
  echo "COCKROACH_ARCHIVE_PREFIX must be set to a non-default value when telemetry is disabled"
  exit 1
fi

version=$(grep -v "^#" "$dir/../pkg/build/version.txt" | head -n1)
if [[ -z "${DRY_RUN}" ]] ; then
  gcr_credentials="$GCS_CREDENTIALS_PROD"
  gcr_staged_repository="us-docker.pkg.dev/releases-prod/cockroachdb-staged-releases/${cockroach_archive_prefix}"
else
  gcr_credentials="$GCS_CREDENTIALS_DEV"
  gcr_staged_repository="us-docker.pkg.dev/releases-dev-356314/cockroachdb-staged-releases/${cockroach_archive_prefix}"
fi
tc_end_block "Variable Setup"


gcr_tag="${gcr_staged_repository}:${version}"
docker_login_gcr "$gcr_staged_repository" "$gcr_credentials"
docker manifest rm "${gcr_tag}" || :
docker manifest create "${gcr_tag}" "${gcr_staged_repository}:amd64-${version}" "${gcr_staged_repository}:arm64-${version}"
docker manifest push "${gcr_tag}"

tc_start_block "Verify docker images"
error=0
for arch in amd64 arm64; do
    tc_start_block "Verify $gcr_tag on $arch"
    if ! verify_docker_image "$gcr_tag" "linux/$arch" "$BUILD_VCS_NUMBER" "$version" false "$telemetry_disabled"; then
      error=1
    fi
    tc_end_block "Verify $gcr_tag on $arch"
done

if [ $error = 1 ]; then
  echo "ERROR: Docker image verification failed, see logs above"
  exit 1
fi
tc_end_block "Verify docker images"
