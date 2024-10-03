#!/usr/bin/env bash

# Copyright 2020 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


source "$(dirname "${0}")/teamcity-support.sh"

# mark_build marks a build with a given label specified as a parameter on
# docker. For example, calling this function on the label "qualified", on a
# v19.2.4 build would tag it as `latest-v19.2-qualified-build`.
mark_build() {
  tc_start_block "Variable Setup"
  build_label=$1

  # On no match, `grep -Eo` returns 1. `|| echo""` makes the script not error.
  release_branch="$(echo "$TC_BUILD_BRANCH" | grep -Eo "^v[0-9]+\.[0-9]+" || echo"")"

  if [[ -z "${DRY_RUN}" ]] ; then
    google_credentials=$GOOGLE_COCKROACH_CLOUD_IMAGES_COCKROACHDB_CREDENTIALS
    gcr_repository="us-docker.pkg.dev/cockroach-cloud-images/cockroachdb/cockroach"
  else
    google_credentials=$GOOGLE_COCKROACH_RELEASE_CREDENTIALS
    gcr_repository="us.gcr.io/cockroach-release/cockroach-test"
  fi
  tc_end_block "Variable Setup"

  tc_start_block "Push new docker image tag"
  if [[ -z "${release_branch}" ]] ; then
    echo "This tag/branch does not contain a valid major version. Tag/Branch=\"${TC_BUILD_BRANCH}\". Unable to tag docker image as qualified."
    exit
  fi

  log_into_gcloud
  gcloud container images add-tag "${gcr_repository}:${TC_BUILD_BRANCH}" "${gcr_repository}:latest-${release_branch}-${build_label}-build"
  tc_end_block "Push new docker image tag"
}

# Publish potential release metadata to a stable location.
publish_qualify_metadata() {
  gcs_bucket="cockroach-release-qualification-test"
  google_credentials=$GCS_CREDENTIALS_DEV
  if [[ -z "${DRY_RUN}" ]] ; then
    gcs_bucket="cockroach-release-qualification-prod"
    google_credentials=$GCS_CREDENTIALS_PROD
  fi

  tc_start_block "Metadata"
  timestamp=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
  metadata_file="artifacts/metadata.json"
  mkdir -p artifacts
  cat > "$metadata_file" << EOF
{
  "sha": "$BUILD_VCS_NUMBER",
  "timestamp": "$timestamp",
  "tag": "$TC_BUILD_BRANCH"
}
EOF
  # Run jq to pretty print and validate JSON
  jq . "$metadata_file"
  log_into_gcloud
  gsutil cp "$metadata_file" "gs://$gcs_bucket/release-qualification/$BUILD_VCS_NUMBER.json"
  tc_end_block "Metadata"
}
