#!/usr/bin/env bash

set -euo pipefail

dir="$(dirname $(dirname $(dirname $(dirname $(dirname "${0}")))))"
source "$dir/release/teamcity-support.sh"
source "$dir/teamcity-bazel-support.sh"  # for run_bazel

tc_start_block "Variable Setup"

build_name=$(git describe --tags --dirty --match=v[0-9]* 2> /dev/null || git rev-parse --short HEAD;)

# On no match, `grep -Eo` returns 1. `|| echo""` makes the script not error.
release_branch="$(echo "$build_name" | grep -Eo "^v[0-9]+\.[0-9]+" || echo"")"
is_custom_build="$(echo "$TC_BUILD_BRANCH" | grep -Eo "^custombuild-" || echo "")"

if [[ -z "${DRY_RUN}" ]] ; then
  google_credentials=$GOOGLE_COCKROACH_CLOUD_IMAGES_COCKROACHDB_CREDENTIALS
  gcr_repository="us-docker.pkg.dev/cockroach-cloud-images/cockroachdb/cockroach"
  # Used for docker login for gcloud
  gcr_hostname="us-docker.pkg.dev"
else
  google_credentials="$GOOGLE_COCKROACH_RELEASE_CREDENTIALS"
  gcr_repository="us.gcr.io/cockroach-release/cockroach-test"
  build_name="${build_name}.dryrun"
  gcr_hostname="us.gcr.io"
fi

cat << EOF

  build_name:      $build_name
  release_branch:  $release_branch
  is_custom_build: $is_custom_build
  gcr_repository:  $gcr_repository

EOF
tc_end_block "Variable Setup"


tc_start_block "Tag the release"
git tag "${build_name}"
tc_end_block "Tag the release"

tc_start_block "Push release tag to github.com/cockroachlabs/release-staging"
github_ssh_key="${GITHUB_COCKROACH_TEAMCITY_PRIVATE_SSH_KEY}"
configure_git_ssh_key
git_wrapped push ssh://git@github.com/cockroachlabs/release-staging.git "${build_name}"
tc_end_block "Push release tag to github.com/cockroachlabs/release-staging"


tc_start_block "Tag docker image as latest-build"
# Only tag the image as "latest-vX.Y-build" if the tag is on a release branch
# (or master for the alphas for the next major release).
if [[ -n "${release_branch}" ]] ; then
  log_into_gcloud
  gcloud container images add-tag "${gcr_repository}:${build_name}" "${gcr_repository}:latest-${release_branch}-build"
fi
if [[ "$TC_BUILD_BRANCH" == "master" ]]; then
  log_into_gcloud
  gcloud container images add-tag "${gcr_repository}:${build_name}" "${gcr_repository}:latest-master-build"
fi
tc_end_block "Tag docker image as latest-build"


# Make finding the tag name easy.
cat << EOF


Build ID: ${build_name}


EOF


if [[ -n "${is_custom_build}" ]] ; then
  tc_start_block "Delete custombuild tag"
  git_wrapped push ssh://git@github.com/cockroachdb/cockroach.git --delete "${TC_BUILD_BRANCH}"
  tc_end_block "Delete custombuild tag"
fi
