#!/usr/bin/env bash

set -euo pipefail

dir="$(dirname $(dirname $(dirname $(dirname $(dirname "${0}")))))"
source "$dir/release/teamcity-support.sh"
source "$dir/teamcity-bazel-support.sh"  # for run_bazel

tc_start_block "Variable Setup"

build_name=$(git describe --tags --dirty --match=v[0-9]* 2> /dev/null || git rev-parse --short HEAD;)

# On no match, `grep -Eo` returns 1. `|| echo""` makes the script not error.
release_branch="$(echo "$build_name" | grep -Eo "^v[0-9]+\.[0-9]+" || echo"")"
is_release_build="$(echo "$TC_BUILD_BRANCH" | grep -Eo "^(release-[0-9][0-9]\.[0-9](\.0)?)$" || echo "")"
is_customized_build="$(echo "$TC_BUILD_BRANCH" | grep -Eo "^custombuild-" || echo "")"
github_ssh_key="${GITHUB_COCKROACH_TEAMCITY_PRIVATE_SSH_KEY}"

if [[ -z "${DRY_RUN}" ]] ; then
  if [[ -z "${is_customized_build}" ]] ; then
    google_credentials=$GOOGLE_COCKROACH_CLOUD_IMAGES_COCKROACHDB_CREDENTIALS
    gcr_repository="us-docker.pkg.dev/cockroach-cloud-images/cockroachdb/cockroach"
    # Used for docker login for gcloud
    gcr_hostname="us-docker.pkg.dev"
    gcs_bucket="cockroach-builds-artifacts-prod"
  else
    gcs_bucket="cockroach-customized-builds-artifacts-prod"
    gcr_repository="us-docker.pkg.dev/cockroach-cloud-images/cockroachdb-customized/cockroach-customized"
  fi
else
  gcs_bucket="cockroach-builds-artifacts-dryrun"
  google_credentials="$GOOGLE_COCKROACH_RELEASE_CREDENTIALS"
  gcr_repository="us.gcr.io/cockroach-release/cockroach-test"
  build_name="${build_name}.dryrun"
  gcr_hostname="us.gcr.io"
fi

cat << EOF

  build_name:          $build_name
  release_branch:      $release_branch
  is_customized_build: $is_customized_build
  gcr_repository:      $gcr_repository

EOF
tc_end_block "Variable Setup"

configure_git_ssh_key

if [[ -z "${is_customized_build}" ]] ; then
  tc_start_block "Tag the release"
  git tag "${build_name}"
  tc_end_block "Tag the release"

  tc_start_block "Push release tag to github.com/cockroachlabs/release-staging"
  git_wrapped push ssh://git@github.com/cockroachlabs/release-staging.git "${build_name}"
  tc_end_block "Push release tag to github.com/cockroachlabs/release-staging"


  tc_start_block "Tag docker image as latest-build"
  # Only tag the image as "latest-vX.Y-build" if the tag is on a release branch
  # (or master for the alphas for the next major release).
  if [[ -n "${is_release_build}" ]] ; then
    log_into_gcloud
    gcloud container images add-tag "${gcr_repository}:${build_name}" "${gcr_repository}:latest-${release_branch}-build"
  fi
  if [[ "$TC_BUILD_BRANCH" == "master" ]]; then
    log_into_gcloud
    gcloud container images add-tag "${gcr_repository}:${build_name}" "${gcr_repository}:latest-master-build"
  fi
tc_end_block "Tag docker image as latest-build"
fi

# Make finding the tag name easy.
cat << EOF


Build ID: ${build_name}

The binaries are available at:
  https://storage.googleapis.com/$gcs_bucket/cockroach-$build_name.linux-amd64.tgz
  https://storage.googleapis.com/$gcs_bucket/cockroach-$build_name.linux-arm64.tgz
  https://storage.googleapis.com/$gcs_bucket/cockroach-$build_name.darwin-10.9-amd64.tgz
  https://storage.googleapis.com/$gcs_bucket/cockroach-$build_name.windows-6.2-amd64.zip

Pull the docker image by:
  docker pull $gcr_repository:$build_name

EOF


if [[ -n "${is_customized_build}" ]] ; then
  tc_start_block "Delete custombuild tag"
  git_wrapped push ssh://git@github.com/cockroachdb/cockroach.git --delete "${TC_BUILD_BRANCH}"
  tc_end_block "Delete custombuild tag"
fi
