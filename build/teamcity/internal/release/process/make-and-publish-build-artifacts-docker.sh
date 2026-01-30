#!/usr/bin/env bash

# Copyright 2023 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -euxo pipefail

dir="$(dirname $(dirname $(dirname $(dirname $(dirname "${0}")))))"
source "$dir/release/teamcity-support.sh"
#
# TODO: remove this block after we upgrade to Ubuntu 22.04+
# this is needed to support s390x builds on Ubuntu 20.04 hosts
docker run --privileged --rm tonistiigi/binfmt@sha256:8f58e6214f4cc9dc83ce8f5acad1ece508eb6b20e696a8c1e9f274481982c541 --uninstall qemu-s390x
docker run --privileged --rm tonistiigi/binfmt@sha256:8f58e6214f4cc9dc83ce8f5acad1ece508eb6b20e696a8c1e9f274481982c541 --install s390x
# End of TODO

tc_start_block "Variable Setup"

build_name=$(git describe --tags --dirty --match=v[0-9]* 2> /dev/null || git rev-parse --short HEAD;)
telemetry_disabled="${TELEMETRY_DISABLED:-false}"
cockroach_archive_prefix="${COCKROACH_ARCHIVE_PREFIX:-cockroach}"
if [[ $telemetry_disabled == true && $cockroach_archive_prefix == "cockroach" ]]; then
  echo "COCKROACH_ARCHIVE_PREFIX must be set to a non-default value when telemetry is disabled"
  exit 1
fi

release_build_match="$(is_release_or_master_build "$TC_BUILD_BRANCH")"

if [[ -z "${DRY_RUN}" ]] ; then
  if [[ -z "${release_build_match}" ]] ; then
    google_credentials=$GOOGLE_CREDENTIALS_CUSTOMIZED
    gcr_repository="us-docker.pkg.dev/cockroach-cloud-images/cockroachdb-customized/${cockroach_archive_prefix}-customized"
    gcr_hostname="us-docker.pkg.dev"
  else
    google_credentials=$GOOGLE_COCKROACH_CLOUD_IMAGES_COCKROACHDB_CREDENTIALS
    gcr_repository="us-docker.pkg.dev/cockroach-cloud-images/cockroachdb/${cockroach_archive_prefix}"
    # Used for docker login for gcloud
    gcr_hostname="us-docker.pkg.dev"
  fi
else
  build_name="${build_name}.dryrun"
  google_credentials="$GOOGLE_COCKROACH_RELEASE_CREDENTIALS"
  gcr_repository="us.gcr.io/cockroach-release/${cockroach_archive_prefix}-test"
  gcr_hostname="us.gcr.io"
fi

cat << EOF

  build_name:             $build_name
  gcr_repository:         $gcr_repository
  release_build_match:    $release_build_match

EOF
tc_end_block "Variable Setup"

tc_start_block "Make and push multi-arch docker image"
docker_login_with_google
gcr_tag="${gcr_repository}:${build_name}"
docker manifest rm "${gcr_tag}" || :
docker manifest create "${gcr_tag}" "${gcr_repository}:amd64-${build_name}" "${gcr_repository}:arm64-${build_name}" "${gcr_repository}:s390x-${build_name}"
docker manifest push "${gcr_tag}"
tc_end_block "Make and push multi-arch docker images"
