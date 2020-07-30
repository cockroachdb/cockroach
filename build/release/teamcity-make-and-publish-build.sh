#!/usr/bin/env bash

set -euxo pipefail

source "$(dirname "${0}")/teamcity-support.sh"

tc_start_block "Variable Setup"
export BUILDER_HIDE_GOPATH_SRC=1

build/builder.sh make .buildinfo/tag
build_name="${TAG_NAME:-$(cat .buildinfo/tag)}"
release_branch="$(echo "$build_name" | grep -Eo "^v[0-9]+\.[0-9]+")"

bucket="${BUCKET-cockroach-builds}"

google_credentials=$GOOGLE_COCKROACH_CLOUD_IMAGES_CREDENTIALS
tc_end_block "Variable Setup"


tc_start_block "Tag the release"
git tag "$build_name"
tc_end_block "Tag the release"


tc_start_block "Compile publish-provisional-artifacts"
build/builder.sh go install ./pkg/cmd/publish-provisional-artifacts
tc_end_block "Compile publish-provisional-artifacts"


tc_start_block "Compile and publish S3 artifacts"
build/builder.sh env \
  AWS_ACCESS_KEY_ID="$AWS_ACCESS_KEY_ID" \
  AWS_SECRET_ACCESS_KEY="$AWS_SECRET_ACCESS_KEY" \
  TC_BUILD_BRANCH="$build_name" \
  publish-provisional-artifacts -provisional -release -bucket "$bucket"
tc_end_block "Compile and publish S3 artifacts"


tc_start_block "Make and push docker image"
configure_docker_creds

gcr_hostname="us.gcr.io"
docker_login_with_google

gcr_repository="us.gcr.io/cockroach-cloud-images/cockroach"

# TODO: update publish-provisional-artifacts with option to leave one or more cockroach binaries in the local filesystem
curl -f -s -S -o- "https://${bucket}.s3.amazonaws.com/cockroach-${build_name}.linux-amd64.tgz" | tar xfz - --strip-components 1
cp cockroach build/deploy/cockroach

docker build --no-cache --tag="${gcr_repository}:${build_name}" build/deploy
docker push "${gcr_repository}:${build_name}"
tc_end_block "Make and push docker image"


tc_start_block "Push release tag to github.com/cockroachdb/cockroach"
github_ssh_key="${GITHUB_COCKROACH_TEAMCITY_PRIVATE_SSH_KEY}"
configure_git_ssh_key
push_to_git ssh://git@github.com/cockroachlabs/release-staging.git "$build_name"
tc_end_block "Push release tag to github.com/cockroachdb/cockroach"


tc_start_block "Tag docker image as latest-build"
# Only tag the image as "latest-vX.Y-build" if the tag is on a release branch
# (or master for the alphas for the next major release).
if [[ -n "${release_branch}" ]] ; then
  log_into_gcloud
  gcloud container images add-tag "${gcr_repository}:${build_name}" "${gcr_repository}:latest-${release_branch}-build"
fi
tc_end_block "Tag docker image as latest-build"
