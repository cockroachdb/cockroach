#!/usr/bin/env bash

# Copyright 2022 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -euo pipefail

# This script can be skipped for dry run and customized builds.
is_customized_build="$(echo "$TC_BUILD_BRANCH" | grep -Eo "^custombuild-" || echo "")"
if [[ -n "${DRY_RUN:-}" ]] || [[ -n "${is_customized_build}" ]]; then
  echo "Skipping for dry run or customized build."
  exit 0
fi

dir="$(dirname $(dirname $(dirname $(dirname $(dirname "${0}")))))"
source "$dir/release/teamcity-support.sh"

github_ssh_key="${GITHUB_COCKROACH_TEAMCITY_PRIVATE_SSH_KEY:?GITHUB_COCKROACH_TEAMCITY_PRIVATE_SSH_KEY must be specified}"
metadata_gcs_bucket="cockroach-release-qualification-prod"
metadata_google_credentials="$GCS_CREDENTIALS_PROD"
build_name="$(git describe --tags --dirty --match=v[0-9]* 2> /dev/null || git rev-parse --short HEAD;)"

configure_git_ssh_key
git tag "${build_name}"
git_wrapped push ssh://git@github.com/cockroachlabs/release-staging.git "${build_name}"

# Publish build metadata to a stable location.
timestamp=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
metadata_file="artifacts/metadata.json"
mkdir -p artifacts
cat > "$metadata_file" << EOF
{
  "sha": "$BUILD_VCS_NUMBER",
  "timestamp": "$timestamp",
  "tag": "$build_name"
}
EOF
# Run jq to pretty print and validate JSON
jq . "$metadata_file"
google_credentials=$metadata_google_credentials log_into_gcloud
gsutil cp "$metadata_file" "gs://$metadata_gcs_bucket/builds/$BUILD_VCS_NUMBER.json"
echo "Published to https://storage.googleapis.com/$metadata_gcs_bucket/builds/$BUILD_VCS_NUMBER.json"
