#!/usr/bin/env bash

source "$(dirname "${0}")/teamcity-support.sh"

tc_start_block "Variable Setup"
build_label=$1
release_branch="$(echo "$TC_BUILD_BRANCH" | grep -Eo "^v[0-9]+\.[0-9]+")"

if [[ -z "${DRY_RUN}" ]] ; then
  google_credentials=$GOOGLE_COCKROACH_CLOUD_IMAGES_CREDENTIALS
  gcr_repository="us.gcr.io/cockroach-cloud-images/cockroach"
else
  google_credentials=$GOOGLE_COCKROACH_RELEASE_CREDENTIALS
  gcr_repository="us.gcr.io/cockroach-release/cockroach-test"
fi
tc_end_block "Variable Setup"

tc_start_block "Push new docker image tag"
if [[ -z "${release_branch}" ]] ; then
  echo "This tag tag/branch does not contain a valid major version. Tag/Branch=\"${TC_BUILD_BRANCH}\". Unable to tag docker image as qualified."
  exit
fi

log_into_gcloud
gcloud container images add-tag "${gcr_repository}:${TC_BUILD_BRANCH}" "${gcr_repository}:latest-${release_branch}-${build_label}-build"
tc_end_block "Push new docker image tag"
