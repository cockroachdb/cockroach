#!/usr/bin/env bash

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

# Publish potential release metadata to a stable location. Metadata files can be overwritten by the next
# qualification build. As a part of the week 0 automation procedure, the latest content of this file will be copied
# to another location, a snapshot that will be used for the rest of the release process.
publish_metadata() {
  # TODO: move to a separate file?
  tc_start_block "Metadata"
  # TODO: ignore master builds?
  # echo $TC_BUILD_BRANCH | grep -q alpha && return
  # release-21.2 style branch name
  release_branch=$(echo $TC_BUILD_BRANCH | awk -F- '{print $1}' | sed 's/^v//' | awk -F. '{print "release-" $1 "." $2}')
  # The tag contains a version number which has to be bumped by 1
  release_version=$(echo $TC_BUILD_BRANCH | awk -F- '{print $1}' | awk -F. '{print $1 "." $2 "." $3+1}')
  timestamp=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
  mkdir -p artifacts
  metadata_file="artifacts/$release_version.json"
  cat > "$metadata_file" << EOF
{
  "version": "$release_version",
  "branch": "$release_branch",
  "sha": "$BUILD_VCS_NUMBER",
  "timestamp": "$timestamp",
  "tag": "$TC_BUILD_BRANCH"
}
EOF
  cat "$metadata_file"
  # TODO: verify json by running jq on it
  # TODO: create a GCS buckets
  # TODO: create credentials to publish to the buckets above and add them to TeamCity
  # TODO: add logic to set prod and dev buckets
  # TODO: make sure gsutil is installed on the agents or in the docker image we may want to use
  # gsutil cp "$release_version.json" "gs://$gcs_bucket/release-qualification/$release_version.json"
  tc_end_block "Metadata"
}
