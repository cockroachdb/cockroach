#!/usr/bin/env bash

# Copyright 2023 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -euxo pipefail

dir="$(dirname $(dirname $(dirname $(dirname $(dirname "${0}")))))"
source "$dir/release/teamcity-support.sh"

tc_start_block "Variable Setup"

build_name=$(git describe --tags --dirty --match=v[0-9]* 2> /dev/null || git rev-parse --short HEAD;)

# On no match, `grep -Eo` returns 1. `|| echo""` makes the script not error.
release_build_match="$(is_release_or_master_build "$TC_BUILD_BRANCH")"

if [[ -z "${DRY_RUN}" ]] ; then
  if [[ -z "${release_build_match}" ]] ; then
    google_credentials=$GOOGLE_CREDENTIALS_CUSTOMIZED
    gcr_repository="us-docker.pkg.dev/cockroach-cloud-images/cockroachdb-customized/cockroach-customized"
    gcr_hostname="us-docker.pkg.dev"
  else
    google_credentials=$GOOGLE_COCKROACH_CLOUD_IMAGES_COCKROACHDB_CREDENTIALS
    gcr_repository="us-docker.pkg.dev/cockroach-cloud-images/cockroachdb/cockroach"
    # Used for docker login for gcloud
    gcr_hostname="us-docker.pkg.dev"
  fi
else
  build_name="${build_name}.dryrun"
  google_credentials="$GOOGLE_COCKROACH_RELEASE_CREDENTIALS"
  gcr_repository="us.gcr.io/cockroach-release/cockroach-test"
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
docker manifest create "${gcr_tag}" "${gcr_repository}:amd64-${build_name}" "${gcr_repository}:arm64-${build_name}"
docker manifest push "${gcr_tag}"
tc_end_block "Make and push multi-arch docker images"
