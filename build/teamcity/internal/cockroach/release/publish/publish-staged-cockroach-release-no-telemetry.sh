#!/usr/bin/env bash

# Copyright 2023 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -euxo pipefail

dir="$(dirname $(dirname $(dirname $(dirname $(dirname $(dirname "${0}"))))))"
source "$dir/teamcity-support.sh"  # For log_into_gcloud

tc_start_block "Variable Setup"
version=$(grep -v "^#" "$dir/../pkg/build/version.txt" | head -n1)
cockroach_archive_prefix="${COCKROACH_ARCHIVE_PREFIX:?COCKROACH_ARCHIVE_PREFIX must be set and not set to 'cockroach'}"
if [[ $cockroach_archive_prefix == "cockroach" ]]; then
  echo "COCKROACH_ARCHIVE_PREFIX must be set to a non-default value"
  exit 1
fi

if [[ -z "${DRY_RUN}" ]] ; then
  # TODO: use different buckets here maybe?
  gcs_bucket="cockroach-release-artifacts-prod"
  gcs_staged_bucket="cockroach-release-artifacts-staged-prod"
  # export the variable to avoid shell escaping
  export gcs_credentials="$GCS_CREDENTIALS_PROD"
else
  gcs_bucket="cockroach-release-artifacts-dryrun"
  gcs_staged_bucket="cockroach-release-artifacts-staged-dryrun"
  # export the variable to avoid shell escaping
  export gcs_credentials="$GCS_CREDENTIALS_DEV"
fi
tc_end_block "Variable Setup"


tc_start_block "Copy binaries"
log_into_gcloud
for platform in linux-amd64 linux-arm64; do
  archive="${cockroach_archive_prefix}-${version}.${platform}.tgz"
  gsutil cp "gs://$gcs_staged_bucket/$archive" "gs://$gcs_bucket/$archive"
  gsutil cp "gs://$gcs_staged_bucket/$archive.sha256sum" "gs://$gcs_bucket/$archive.sha256sum"
done
tc_end_block "Copy binaries"
